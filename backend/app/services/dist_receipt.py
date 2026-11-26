"""分发回执中心领域服务：管理订阅方分发回执的回收核对、差异标记与重发请求。

数据链路位置：分发层对账端，负责消费分发批次的送达结果，
把回执差异回写给分发任务与订阅方。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class DistReceiptService(BaseModuleService):
    MODULE = "dist_receipt"
    ENTITY = "回执记录"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["回执编号", "分发批次", "订阅方", "回执状态", "差异说明"]
    REQUIRED_FIELDS = ["回执编号", "分发批次", "订阅方"]
    STATUS_ORDER = ["已回收", "部分回收", "未回收"]
    ACTION_RULES = {"重发回执请求": "部分回收", "标记差异": "未回收"}
    NEGATIVE_ACTIONS = []

    # ---- 业务扩展：回执核对与差异处置 ----

    def verify_receipt(self, entry_id: int) -> dict[str, Any]:
        """核对回执：回滚终态回执按已确定的登记结算，其余按分发批次校验。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "回执记录不存在"}
        terminal = self._rollback_terminal(entry)
        if terminal is not None:
            # 撤回/取消/失败回执是终态事实：再次到达只回放登记，不改写状态。
            return {
                "ok": True,
                "message": f"回滚终态回执 {entry.get('回执编号', '')} 已存在，按已确定的 {terminal} 登记结算",
                "receipt": self._receipt(entry, "核对回执", True, terminal),
            }
        batch = entry.get("分发批次", "")
        task = next((t for t in store.rows("dist_task")
                     if t.get("分发批次") == batch), None)
        if task is None:
            entry["status"] = "未回收"
            entry["差异说明"] = "分发批次不存在"
            return {"ok": False, "message": f"分发批次 {batch} 不存在"}
        if task.get("status") == "已送达":
            entry["status"] = "已回收"
            entry["pending"] = False
            return {"ok": True, "message": "回执与分发结果一致"}
        entry["status"] = "部分回收"
        entry["差异说明"] = f"分发状态为 {task.get('status')}"
        return {"ok": False, "message": "回执与分发结果不一致"}

    def mark_diff(self, entry_id: int, diff: str) -> dict[str, Any]:
        """标记回执差异；撤回回执不能被后来的取消或差异标记抹掉。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "回执记录不存在"}
        terminal = self._rollback_terminal(entry)
        if terminal is not None:
            return {
                "ok": False,
                "message": f"回执 {entry.get('回执编号', '')} 是已确定的 {terminal} 终态回执，"
                "差异标记未获处理资格，撤回回执保持不变",
                "receipt": self._receipt(entry, "标记差异", False, terminal),
            }
        entry["status"] = "未回收"
        entry["差异说明"] = diff
        entry["abnormal"] = True
        batch = entry.get("分发批次", "")
        for task in store.rows("dist_task"):
            if task.get("分发批次") == batch:
                task["status"] = "分发失败"
                task["abnormal"] = True
        return {"ok": True, "message": f"差异已标记: {diff}"}

    def perform_action(
        self, entry_id: int, action: str, remark: str | None = None
    ) -> dict[str, Any]:
        if action == "标记差异":
            return self.mark_diff(entry_id, remark or "订阅方回执与分发结果不一致")
        if action == "重发回执请求":
            return self.resend_request(entry_id)
        return super().perform_action(entry_id, action, remark)

    def _rollback_terminal(self, entry: dict[str, Any]) -> dict[str, Any] | None:
        """识别回滚终态回执：撤回确认 / 取消 / 失败三类分别保留。"""
        code = str(entry.get("回执编号", ""))
        terminal = str(entry.get("终态", ""))
        if code.startswith(("rbr-", "rbc-", "rbf-")) or terminal in ("已生效", "已取消", "回滚失败"):
            if not terminal:
                if code.startswith("rbr-"):
                    terminal = "已生效"
                elif code.startswith("rbc-"):
                    terminal = "已取消"
                else:
                    terminal = "回滚失败"
            return terminal
        return None

    def _receipt(
        self, entry: dict[str, Any], action: str, accepted: bool, terminal: str
    ) -> dict[str, Any]:
        return {
            "动作": action,
            "办理结果": "已办理" if accepted else "未受理",
            "终态": terminal,
            "确认回执": str(entry.get("回执编号", "")),
            "说明": str(entry.get("差异说明", "")),
        }

    def resend_request(self, entry_id: int) -> dict[str, Any]:
        """重发回执请求并等待订阅方回传；撤回回执不重发。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "回执记录不存在"}
        if self._rollback_terminal(entry) is not None:
            return {
                "ok": False,
                "message": "撤回终态回执不可重发，已确定内容保持不变",
                "receipt": self._receipt(entry, "重发回执请求", False, "已生效"),
            }
        entry["status"] = "部分回收"
        entry["pending"] = True
        entry["重发时间"] = datetime.now(timezone.utc).isoformat()
        return {"ok": True, "message": "回执请求已重发"}

    def consume_dist_results(self, entry_id: int) -> list[dict[str, Any]]:
        """读取分发模块的送达批次，作为核对输入。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return []
        subscriber = entry.get("订阅方", "")
        return [t for t in store.rows("dist_task")
                if t.get("订阅方") == subscriber and t.get("status") in ("已送达", "分发失败")]

    def stats_by_subscriber(self) -> dict[str, dict[str, int]]:
        """按订阅方统计回执回收情况，供对账看板消费。"""
        stats: dict[str, dict[str, int]] = {}
        for row in store.rows(self.MODULE):
            subscriber = row.get("订阅方", "未知")
            bucket = stats.setdefault(subscriber, {"已回收": 0, "部分回收": 0, "未回收": 0})
            status = row.get("status", "未回收")
            if status in bucket:
                bucket[status] += 1
        return stats

    def notify_receipt_gap(self, entry_id: int) -> str:
        """把回执缺口通知订阅方重新上传。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return "回执记录不存在"
        subscriber = entry.get("订阅方", "")
        for sub in store.rows("subscriber_mgr"):
            if sub.get("订阅方标识") == subscriber:
                sub["pending"] = True
        return f"已通知 {subscriber} 补传回执"
