"""分发任务领域服务：管理把发布版本分发给订阅方的批次调度、送达确认与失败重试，完成导出动作。

数据链路位置：主数据分发层执行端，负责消费发布版本与订阅过滤，
把送达回执写入回执中心。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services import rollback_ledger
from app.services.base import BaseModuleService
from app.store import store


class DistTaskService(BaseModuleService):
    MODULE = "dist_task"
    ENTITY = "分发批次"
    OPERATION_TERM = "导出"
    LIST_FIELDS = ["分发批次", "订阅方", "记录范围", "分发状态", "回执状态"]
    REQUIRED_FIELDS = ["分发批次", "订阅方", "记录范围"]
    STATUS_ORDER = ["排队中", "分发中", "已送达", "分发失败"]
    ACTION_RULES = {"开始分发": "分发中", "确认送达": "已送达", "重试分发": "排队中"}
    NEGATIVE_ACTIONS = []

    # ---- 统一动作入口：领取前先读回滚终态确认 ----

    def perform_action(
        self, entry_id: int, action: str, remark: str | None = None
    ) -> dict[str, Any]:
        if action == "开始分发":
            return self.dispatch_batch(entry_id)
        if action == "确认送达":
            return self.confirm_delivery(entry_id)
        if action == "重试分发":
            return self.retry_dispatch(entry_id, remark or "操作员重试分发")
        return super().perform_action(entry_id, action, remark)

    # ---- 业务扩展：版本分发与回执回收 ----

    def dispatch_batch(self, entry_id: int) -> dict[str, Any]:
        """执行分发（领取入口）：校验订阅方生效且版本未被撤回。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return self._dispatch_receipt(
                None, "开始分发", False, f"分发批次 {entry_id} 不存在", status=""
            )
        version = self._scope_version(entry)
        if rollback_ledger.is_withdrawn(version):
            confirmation = rollback_ledger.find_confirmation(version)
            code = confirmation.get("回执编号", "") if confirmation else ""
            return self._dispatch_receipt(
                entry,
                "开始分发",
                False,
                f"版本 {version} 已撤回（{code}），金记录发布端不再认定为可领取；"
                "若确需重新启用，须先另起一次发布批次并登记依据",
                status="已撤回",
                receipt_override=confirmation,
            )
        if entry.get("status") in ("已送达", "分发中"):
            return self._dispatch_receipt(
                entry,
                "开始分发",
                entry.get("status") == "已送达",
                f"分发批次当前终态为 {entry.get('status')}，按既有回执结算",
                status=str(entry.get("status", "")),
            )
        subscriber = entry.get("订阅方", "")
        sub = next((s for s in store.rows("subscriber_mgr")
                    if s.get("订阅方标识") == subscriber), None)
        if sub is None or sub.get("status") != "已生效":
            return self._dispatch_receipt(
                entry, "开始分发", False, f"订阅方 {subscriber} 未生效，拒绝分发", status=entry.get("status", "")
            )
        entry["status"] = "分发中"
        entry["分发状态"] = "分发中"
        entry["pending"] = True
        return self._dispatch_receipt(
            entry,
            "开始分发",
            True,
            f"版本 {version} 仍在领取范围内，已进入分发",
            status="分发中",
        )

    def retry_dispatch(self, entry_id: int, reason: str) -> dict[str, Any]:
        """重试分发同样先读终态确认，已撤版本不能因重试再次领取。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return self._dispatch_receipt(
                None, "重试分发", False, f"分发批次 {entry_id} 不存在", status=""
            )
        version = self._scope_version(entry)
        confirmation = rollback_ledger.find_confirmation(version)
        if rollback_ledger.is_withdrawn(version):
            return self._dispatch_receipt(
                entry,
                "重试分发",
                False,
                f"版本 {version} 已撤回，不接受重试分发",
                status="已撤回",
                receipt_override=confirmation,
            )
        if entry.get("status") != "分发失败":
            return self._dispatch_receipt(
                entry,
                "重试分发",
                False,
                f"分发批次当前终态为 {entry.get('status')}，不具备重试资格",
                status=str(entry.get("status", "")),
            )
        entry["status"] = "排队中"
        entry["pending"] = True
        entry["abnormal"] = False
        entry["失败原因"] = reason
        return self._dispatch_receipt(
            entry, "重试分发", True, "已重新排队，等待下一轮领取", status="排队中"
        )

    def confirm_delivery(self, entry_id: int) -> dict[str, Any]:
        """确认送达并向回执中心登记已回收回执。

        迟到的执行回执不能覆盖已经确定的撤回终态：版本已撤回时，
        本次回执单独说明未获处理资格，不把分发批次改写成已送达。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return self._dispatch_receipt(
                None, "确认送达", False, f"分发批次 {entry_id} 不存在", status=""
            )
        version = self._scope_version(entry)
        if rollback_ledger.is_withdrawn(version):
            confirmation = rollback_ledger.find_confirmation(version)
            code = confirmation.get("回执编号", "") if confirmation else ""
            entry["迟到回执"] = "未获处理资格：版本已撤回"
            return self._dispatch_receipt(
                entry,
                "确认送达",
                False,
                f"执行回执迟到：版本 {version} 的撤回确认 {code} 已登记，"
                "本次送达回执未获处理资格，撤回回执保持不变",
                status="已撤回",
                receipt_override=confirmation,
            )
        if entry.get("status") != "分发中":
            return self._dispatch_receipt(
                entry,
                "确认送达",
                False,
                f"批次不在分发中状态（当前 {entry.get('status')}），回执未获处理资格",
                status=str(entry.get("status", "")),
            )
        entry["status"] = "已送达"
        entry["分发状态"] = "已送达"
        entry["回执状态"] = "已回收"
        entry["pending"] = False
        receipts = store.rows("dist_receipt")
        receipt_id = max((int(r.get("id", 0)) for r in receipts), default=0) + 1
        receipts.append({
            "id": receipt_id,
            "回执编号": f"rc-{entry_id}-{receipt_id}",
            "分发批次": entry.get("分发批次", ""),
            "订阅方": entry.get("订阅方", ""),
            "回执状态": "已回收",
            "差异说明": "",
            "status": "已回收",
            "pending": False,
            "abnormal": False,
        })
        return self._dispatch_receipt(
            entry,
            "确认送达",
            True,
            "送达回执已回收",
            status="已送达",
            code=f"rc-{entry_id}-{receipt_id}",
        )

    def fail_and_requeue(self, entry_id: int, reason: str) -> dict[str, Any]:
        """标记分发失败，保留断点供重试。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "分发批次不存在"}
        entry["status"] = "分发失败"
        entry["abnormal"] = True
        entry["失败原因"] = reason
        return {"ok": False, "message": f"分发失败: {reason}", "retryable": True}

    def consume_publish_scope(self, entry_id: int) -> dict[str, Any] | None:
        """读取发布模块的版本范围，校验分发覆盖；已撤版本返回 None。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        scope = self._scope_version(entry)
        if rollback_ledger.is_withdrawn(scope):
            return None
        return next((p for p in store.rows("golden_publish")
                     if p.get("版本标识") == scope), None)

    def resolve_backlog(self, entry_id: int) -> int:
        """按发布批次聚合积压分发任务数。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return 0
        scope = str(entry.get("记录范围", "")).replace("版本 ", "")
        return sum(1 for t in store.rows(self.MODULE)
                   if str(t.get("记录范围", "")).endswith(scope)
                   and t.get("status") in ("排队中", "分发中"))

    def withdraw_from_subscribers(self, entry_id: int, confirmation_code: str) -> list[str]:
        """按撤回确认通知订阅方：已撤版本不再领取，未送达批次转为撤回终结。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return []
        version = self._scope_version(entry)
        if not version or not rollback_ledger.is_withdrawn(version):
            return []
        notified: list[str] = []
        for task in store.rows(self.MODULE):
            if self._scope_version(task) != version:
                continue
            subscriber = str(task.get("订阅方", ""))
            if subscriber and subscriber not in notified:
                notified.append(subscriber)
            if task.get("status") in ("已送达", "分发失败"):
                # 历史送达回执不改写：它记录的是撤回前的实际送达事实。
                continue
            task["status"] = "分发失败"
            task["分发状态"] = "分发失败"
            task["回执状态"] = "撤回终结"
            task["撤回确认"] = confirmation_code
            task["abnormal"] = True
            task["pending"] = False
        entry["撤回确认"] = confirmation_code
        return notified

    def notify_subscriber_rollback(self, entry_id: int, new_version: str) -> str:
        """回滚后通知订阅方新版本口径；新版本必须来自另起的发布批次。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return "分发批次不存在"
        if rollback_ledger.is_withdrawn(new_version):
            return f"版本 {new_version} 已撤回，不能作为回滚后的新版本口径"
        old_version = self._scope_version(entry)
        if old_version and not rollback_ledger.is_withdrawn(old_version):
            return f"版本 {old_version} 未撤回，无需切换口径"
        subscriber = entry.get("订阅方", "")
        for sub in store.rows("subscriber_mgr"):
            if sub.get("订阅方标识") == subscriber:
                sub["pending"] = True
        entry["原版本"] = old_version
        entry["记录范围"] = f"版本 {new_version}"
        entry["status"] = "排队中"
        entry["回执状态"] = "未回收"
        entry["pending"] = True
        return f"已通知 {subscriber} 切换到 {new_version}"

    # ---- 内部：范围版本与终态回执 ----

    def _scope_version(self, entry: dict[str, Any]) -> str:
        return str(entry.get("记录范围", "")).replace("版本 ", "").strip()

    def _dispatch_receipt(
        self,
        entry: dict[str, Any] | None,
        action: str,
        accepted: bool,
        message: str,
        *,
        status: str,
        code: str = "",
        receipt_override: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        version = self._scope_version(entry) if entry else ""
        if receipt_override is not None:
            code = str(receipt_override.get("回执编号", ""))
        return {
            "ok": accepted,
            "message": message,
            "entry": entry,
            "action": action,
            "receipt": {
                "动作": action,
                "办理结果": "已办理" if accepted else "未受理",
                "终态": status,
                "版本标识": version,
                "确认回执": code,
                "说明": message,
            },
        }
