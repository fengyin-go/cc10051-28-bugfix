"""版本回滚器领域服务：管理发布版本回滚任务的执行、影响订阅方通知与取消终结。

数据链路位置：发布层逆向通道，负责消费发布历史与分发记录，
把回滚结果通知订阅方并登记唯一一份终态确认依据。

关键约束：
- 执行回滚成功后，版本的撤回确认即已登记，取消不能把它恢复成可领取；
- 取消只对「待执行」任务有资格；已回滚/已失败/已取消的任务各自返回终态回执；
- 同一回执重复到达时按已登记的终态结算，不重复办理、不覆盖既有结论；
- 业务确需重新启用某个已撤回版本时，必须另起一条发布批次并登记依据，
  本模块不提供「取消即恢复」的路径。
"""
from __future__ import annotations

from typing import Any

from app.services import rollback_ledger
from app.services.base import BaseModuleService
from app.store import store


class VersionRollbackService(BaseModuleService):
    MODULE = "version_rollback"
    ENTITY = "回滚任务"
    OPERATION_TERM = "迁移"
    LIST_FIELDS = ["回滚任务", "目标版本", "回滚原因", "执行状态", "影响订阅方"]
    REQUIRED_FIELDS = ["回滚任务", "目标版本", "回滚原因"]
    STATUS_ORDER = ["待执行", "已回滚", "已取消", "回滚失败"]
    ACTION_RULES = {"执行回滚": "已回滚", "取消任务": "已取消"}
    NEGATIVE_ACTIONS = []
    # 取消是独立终态，不再借用「回滚失败」表达，避免页面把取消误读为失败。
    TERMINAL_STATUSES = {"已回滚", "回滚失败", "已取消"}

    # ---- 统一动作入口：页面按这里返回的服务端确认结果展示终态回执 ----

    def perform_action(
        self, entry_id: int, action: str, remark: str | None = None
    ) -> dict[str, Any]:
        """按动作分派执行回滚与取消，两条入口都做资格校验并落终态回执。"""
        if action == "执行回滚":
            return self.execute_rollback(entry_id)
        if action == "取消任务":
            return self.cancel_task(entry_id, remark or "操作员取消回滚任务")
        return super().perform_action(entry_id, action, remark)

    # ---- 执行回滚：确认撤回并登记唯一确认依据 ----

    def execute_rollback(self, entry_id: int) -> dict[str, Any]:
        """执行回滚：校验任务资格与目标版本存在后切换发布状态并登记撤回回执。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return self._rejected(None, "执行回滚", f"回滚任务 {entry_id} 不存在")
        existing = self._existing_terminal(entry)
        if existing is not None:
            # 同一任务的回执再次到达：按已确定的回滚登记结算，不重复办理。
            return existing
        if entry.get("status") != "待执行":
            return self._rejected(
                entry, "执行回滚", f"回滚任务当前终态为 {entry.get('status')}，不具备执行资格"
            )
        target = str(entry.get("目标版本", ""))
        publish = next((p for p in store.rows("golden_publish")
                        if p.get("版本标识") == target), None)
        if publish is None:
            return self._fail(entry, f"目标版本 {target} 不存在")
        if publish.get("status") == "已回滚":
            # 版本已由更早的回滚完成任务撤回：本任务仍未执行，按已有确认登记结算。
            confirmation = rollback_ledger.find_confirmation(target)
            return self._rejected(
                entry,
                "执行回滚",
                f"目标版本 {target} 已被回滚撤回，撤回确认 {confirmation.get('回执编号', '') if confirmation else ''} 已登记，不再重复办理",
                receipt=confirmation,
            )
        publish["status"] = "已回滚"
        publish["abnormal"] = True
        entry["status"] = "已回滚"
        entry["执行状态"] = "已回滚"
        entry["pending"] = False
        entry["abnormal"] = False
        notified = self.notify_subscribers(entry_id)
        entry["影响订阅方"] = ",".join(notified)
        confirmation = rollback_ledger.register_confirmation(
            entry, subscribers=notified
        )
        entry["确认回执"] = confirmation.get("回执编号", "")
        entry["确认依据"] = confirmation.get("回执编号", "")
        entry["终态"] = "已生效"
        return self._accepted(
            entry,
            f"回滚已生效，撤回确认 {confirmation.get('回执编号', '')} 已登记；"
            "取消操作不能恢复该版本的领取资格，如确需重新启用请另起一次发布批次",
            receipt=confirmation,
        )

    def notify_subscribers(self, entry_id: int) -> list[str]:
        """回滚后通知受影响订阅方并停掉该版本的分发批次，返回通知名单。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "已回滚":
            return []
        target = str(entry.get("目标版本", ""))
        notified: list[str] = []
        for task in store.rows("dist_task"):
            if not str(task.get("记录范围", "")).endswith(target):
                continue
            if task.get("status") in ("已送达", "分发失败"):
                # 已送达/已失败的回执是历史事实，回滚只做通知不改写旧回执。
                subscriber = task.get("订阅方", "")
                if subscriber and subscriber not in notified:
                    notified.append(subscriber)
                continue
            subscriber = task.get("订阅方", "")
            if subscriber and subscriber not in notified:
                notified.append(subscriber)
            task["status"] = "分发失败"
            task["分发状态"] = "分发失败"
            task["回执状态"] = "撤回终结"
            task["abnormal"] = True
            task["pending"] = False
            task["失败原因"] = f"版本 {target} 已撤回"
        entry["影响订阅方"] = ",".join(notified)
        return notified

    def consume_publish_history(self, entry_id: int) -> list[dict[str, Any]]:
        """读取发布历史，选择可回滚版本。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return []
        return [p for p in store.rows("golden_publish")
                if p.get("status") == "已发布"]

    def estimate_impact(self, entry_id: int) -> dict[str, Any]:
        """估算回滚影响面：受影响订阅方与分发批次数。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {}
        target = str(entry.get("目标版本", ""))
        affected = [t for t in store.rows("dist_task")
                    if str(t.get("记录范围", "")).endswith(target)]
        subscribers = {t.get("订阅方", "") for t in affected}
        return {"affected_batches": len(affected), "affected_subscribers": len(subscribers)}

    # ---- 取消任务：只终结待执行任务，绝不恢复已撤回版本 ----

    def cancel_task(self, entry_id: int, reason: str) -> dict[str, Any]:
        """取消回滚任务：仅待执行任务可取消；已撤回版本不会被恢复领取。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return self._rejected(None, "取消任务", f"回滚任务 {entry_id} 不存在")
        status = entry.get("status")
        if status == "已回滚":
            # 已经完成撤回的版本不能靠取消回滚恢复领取，只返回既有撤回回执。
            confirmation = rollback_ledger.find_confirmation(str(entry.get("目标版本", "")))
            return self._rejected(
                entry,
                "取消任务",
                "取消未受理：回滚已生效且撤回确认已登记，该版本不能恢复领取；"
                "如确需重新启用，请另起一次发布批次并登记依据",
                receipt=confirmation,
            )
        if status == "已取消":
            cancel = rollback_ledger.find_cancel(entry)
            return self._accepted(
                entry,
                "取消回执已存在，按首次取消登记结算",
                receipt=cancel,
            )
        if status == "回滚失败":
            failure = rollback_ledger.find_failure(entry)
            return self._rejected(
                entry,
                "取消任务",
                f"取消未受理：回滚任务已失败终结（{entry.get('失败回执', '')}），"
                "取消只对处于待执行的任务有资格",
                receipt=failure,
            )
        if status != "待执行":
            return self._rejected(
                entry, "取消任务", f"回滚任务当前终态为 {status}，不具备取消资格"
            )
        entry["status"] = "已取消"
        entry["执行状态"] = "已取消"
        entry["取消原因"] = reason
        entry["终态"] = "已取消"
        entry["pending"] = False
        entry["abnormal"] = False
        # 待执行任务取消：目标版本从未撤回，发布批次保持原状（可领取）。
        cancel = rollback_ledger.register_cancel(entry, reason)
        entry["取消回执"] = cancel.get("回执编号", "")
        return self._accepted(
            entry,
            f"回滚任务已取消（{cancel.get('回执编号', '')}），目标版本未被撤回，仍保持原有领取资格",
            receipt=cancel,
        )

    def record_rollback_receipt(self, entry_id: int) -> int:
        """登记（或返回）回执中心里的回滚终态回执编号。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return 0
        version = str(entry.get("目标版本", ""))
        confirmation = rollback_ledger.find_confirmation(version)
        if confirmation is not None:
            return int(confirmation.get("回执中心记录", 0) or 0)
        cancel = rollback_ledger.find_cancel(entry)
        if cancel is not None:
            return int(cancel.get("id", 0) or 0)
        failure = rollback_ledger.find_failure(entry)
        if failure is not None:
            return int(failure.get("id", 0) or 0)
        return 0

    # ---- 内部：终态回执与资格说明 ----

    def _existing_terminal(self, entry: dict[str, Any]) -> dict[str, Any] | None:
        """按任务当前终态返回既有回执；同一回执重复到达时直接结算。"""
        status = entry.get("status")
        if status == "已回滚":
            version = str(entry.get("目标版本", ""))
            confirmation = rollback_ledger.find_confirmation(version)
            code = (
                confirmation.get("回执编号", "")
                if confirmation is not None
                else str(entry.get("确认回执", ""))
            )
            return self._accepted(
                entry,
                f"回滚已生效，按已登记的撤回确认 {code} 结算",
                receipt=confirmation,
            )
        if status == "回滚失败":
            failure = rollback_ledger.find_failure(entry)
            if failure is not None:
                return self._rejected(
                    entry,
                    "执行回滚",
                    f"回滚已失败（{failure.get('回执编号', '')}）：{failure.get('失败原因', '')}",
                    receipt=failure,
                )
        return None

    def _accepted(
        self, entry: dict[str, Any], message: str, *, receipt: dict[str, Any] | None
    ) -> dict[str, Any]:
        return {
            "ok": True,
            "message": message,
            "entry": entry,
            "action": self._action_for(entry),
            "receipt": self._receipt(entry, receipt, accepted=True, reason=message),
        }

    def _rejected(
        self,
        entry: dict[str, Any] | int | None,
        action: str,
        message: str,
        *,
        receipt: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        entry_row = entry if isinstance(entry, dict) else None
        return {
            "ok": False,
            "message": message,
            "entry": entry_row,
            "action": action,
            "receipt": self._receipt(
                entry_row, receipt, accepted=False, reason=message, action=action
            ),
        }

    def _fail(self, entry: dict[str, Any], reason: str) -> dict[str, Any]:
        entry["status"] = "回滚失败"
        entry["执行状态"] = "回滚失败"
        entry["失败原因"] = reason
        entry["pending"] = False
        entry["abnormal"] = True
        entry["终态"] = "回滚失败"
        failure = rollback_ledger.register_failure(entry, reason)
        entry["失败回执"] = failure.get("回执编号", "")
        return self._rejected(
            entry, "执行回滚", f"回滚执行失败：{reason}", receipt=failure
        )

    def _action_for(self, entry: dict[str, Any]) -> str:
        return "取消任务" if entry.get("status") == "已取消" else "执行回滚"

    def _receipt(
        self,
        entry: dict[str, Any] | None,
        receipt: dict[str, Any] | None,
        *,
        accepted: bool,
        reason: str,
        action: str | None = None,
    ) -> dict[str, Any]:
        terminal = ""
        code = ""
        version = ""
        if receipt is not None:
            terminal = str(receipt.get("终态", ""))
            code = str(receipt.get("回执编号", ""))
            version = str(receipt.get("目标版本", ""))
        elif entry is not None:
            terminal = str(entry.get("终态", "") or entry.get("status", ""))
            version = str(entry.get("目标版本", ""))
        return {
            "动作": action or (self._action_for(entry) if entry else ""),
            "办理结果": "已办理" if accepted else "未受理",
            "终态": terminal,
            "目标版本": version,
            "确认回执": code,
            "说明": reason,
        }
