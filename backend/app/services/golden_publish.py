"""金记录发布领域服务：管理金记录版本的发布批次、版本标识、扇出分发与回滚衔接，完成入库动作。

数据链路位置：主数据发布层，负责消费合并产物，把发布版本
扇出给订阅方并衔接版本回滚任务。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services import rollback_ledger
from app.services.base import BaseModuleService
from app.store import store


class GoldenPublishService(BaseModuleService):
    MODULE = "golden_publish"
    ENTITY = "发布批次"
    OPERATION_TERM = "入库"
    LIST_FIELDS = ["发布批次", "实体编码", "金记录数", "发布时间", "版本标识"]
    REQUIRED_FIELDS = ["发布批次", "实体编码", "金记录数"]
    STATUS_ORDER = ["待发布", "已发布", "已回滚"]
    ACTION_RULES = {"发布批次": "已发布", "回滚批次": "已回滚"}
    NEGATIVE_ACTIONS = ["回滚批次"]

    # ---- 统一动作入口：发布与回滚都按终态回执结算 ----

    def perform_action(
        self, entry_id: int, action: str, remark: str | None = None
    ) -> dict[str, Any]:
        if action == "发布批次":
            return self.release_version(entry_id, basis=remark)
        if action == "回滚批次":
            return self.rollback_version(entry_id, remark or "操作员回滚发布批次")
        return super().perform_action(entry_id, action, remark)

    # ---- 业务扩展：金记录发布与分发衔接 ----

    def release_version(self, entry_id: int, basis: str | None = None) -> dict[str, Any]:
        """发布金记录版本：已撤版本必须另起新批次并登记独立依据才可发布。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return self._publish_receipt(
                None, "发布批次", False, f"发布批次 {entry_id} 不存在", status=""
            )
        status = entry.get("status")
        if status == "已发布":
            return self._publish_receipt(
                entry,
                "发布批次",
                True,
                f"批次已发布，版本 {entry.get('版本标识', '')} 按既有发布回执结算",
                status="已生效",
            )
        if status == "已回滚":
            confirmation = rollback_ledger.find_confirmation(str(entry.get("版本标识", "")))
            code = confirmation.get("回执编号", "") if confirmation else ""
            return self._publish_receipt(
                entry,
                "发布批次",
                False,
                f"该批次已撤回（{code}），已撤的原版本不能重新发给订阅方；"
                "如确需重新启用，请另外登记一次发布批次并写明独立依据",
                status="已撤回",
                receipt_override=confirmation,
            )
        if status != "待发布":
            return self._publish_receipt(
                entry, "发布批次", False, f"发布批次当前终态为 {status}，不具备发布资格", status=status
            )
        entity = str(entry.get("实体编码", ""))
        prior_withdrawn = rollback_ledger.withdrawn_versions(entity) if entity else []
        supplied_basis = str(basis or entry.get("发布依据") or entry.get("启用依据") or "").strip()
        if prior_withdrawn and not supplied_basis:
            return self._publish_receipt(
                entry,
                "发布批次",
                False,
                f"实体 {entity} 已有撤回版本（{'、'.join(prior_withdrawn)}）；"
                "重新启用必须另外登记一次发布批次并写明独立依据",
                status="待发布",
            )
        now = datetime.now(timezone.utc).isoformat()
        version = f"pub-v{entry_id}-{now[:10]}"
        entry["发布时间"] = now
        entry["版本标识"] = version
        entry["status"] = "已发布"
        entry["pending"] = False
        entry["发布依据"] = supplied_basis or "初次发布"
        entry["重新启用"] = bool(prior_withdrawn)
        return self._publish_receipt(
            entry,
            "发布批次",
            True,
            f"版本 {version} 已发布，领取范围按本次发布终态确认"
            + (f"；重新启用依据：{entry['发布依据']}" if prior_withdrawn else ""),
            status="已生效",
        )

    def _publish_receipt(
        self,
        entry: dict[str, Any] | None,
        action: str,
        accepted: bool,
        message: str,
        *,
        status: str,
        receipt_override: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        version = str(entry.get("版本标识", "")) if entry else ""
        code = ""
        if receipt_override is not None:
            code = str(receipt_override.get("回执编号", ""))
        elif accepted and entry is not None:
            code = f"pbr-{version}" if version else ""
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

    def fanout_to_subscribers(self, entry_id: int) -> list[int]:
        """把已发布版本扇出给全部生效订阅方，生成分发批次。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "已发布":
            return []
        version = str(entry.get("版本标识", ""))
        if rollback_ledger.is_withdrawn(version):
            # 已撤版本不再扇出：领取范围以回滚终态确认为准。
            return []
        tasks = store.rows("dist_task")
        created: list[int] = []
        for sub in store.rows("subscriber_mgr"):
            if sub.get("status") != "已生效":
                continue
            task_id = max((int(r.get("id", 0)) for r in tasks), default=0) + 1
            tasks.append({
                "id": task_id,
                "分发批次": f"dist-{entry_id}-{task_id}",
                "订阅方": sub.get("订阅方标识", ""),
                "记录范围": f"版本 {version}",
                "分发状态": "排队中",
                "回执状态": "未回收",
                "status": "排队中",
                "pending": True,
                "abnormal": False,
            })
            created.append(task_id)
        return created

    def rollback_version(self, entry_id: int, reason: str) -> dict[str, Any]:
        """回滚发布版本并触发版本回滚任务。

        回滚只登记任务，真正的撤回要等版本回滚器执行完成并确认；
        在此之前取消任务，版本保持可领取。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return self._publish_receipt(
                None, "回滚批次", False, f"发布批次 {entry_id} 不存在", status=""
            )
        if entry.get("status") == "已回滚":
            confirmation = rollback_ledger.find_confirmation(str(entry.get("版本标识", "")))
            return self._publish_receipt(
                entry,
                "回滚批次",
                False,
                "该批次已撤回，回滚登记已存在，不重复办理",
                status="已撤回",
                receipt_override=confirmation,
            )
        if entry.get("status") != "已发布":
            return self._publish_receipt(
                entry, "回滚批次", False, "批次未发布，无法回滚", status=entry.get("status", "")
            )
        rollbacks = store.rows("version_rollback")
        existing = next(
            (
                r
                for r in rollbacks
                if str(r.get("目标版本", "")) == str(entry.get("版本标识", ""))
            ),
            None,
        )
        if existing is not None:
            return self._publish_receipt(
                entry,
                "回滚批次",
                False,
                f"该版本已存在回滚任务（{existing.get('回滚任务', '')}），不重复登记",
                status=str(existing.get("status", "")),
            )
        rb_id = max((int(r.get("id", 0)) for r in rollbacks), default=0) + 1
        rollbacks.append({
            "id": rb_id,
            "回滚任务": f"rb-{entry_id}-{rb_id}",
            "目标版本": entry.get("版本标识", ""),
            "回滚原因": reason,
            "执行状态": "待执行",
            "影响订阅方": "",
            "status": "待执行",
            "pending": True,
            "abnormal": True,
        })
        return {
            "ok": True,
            "message": "回滚任务已创建，等待版本回滚器执行确认",
            "entry": entry,
            "action": "回滚批次",
            "rollback_id": rb_id,
            "receipt": {
                "动作": "回滚批次",
                "办理结果": "已办理",
                "终态": "待执行",
                "版本标识": str(entry.get("版本标识", "")),
                "确认回执": f"rbt-{entry.get('版本标识', '')}",
                "说明": "回滚任务待执行；执行确认前版本仍按已发布结算",
            },
        }

    def consume_merge_output(self, entry_id: int) -> list[dict[str, Any]]:
        """读取合并模块产出的金记录，作为发布输入。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return []
        return [m for m in store.rows("golden_merge")
                if m.get("实体编码") == entry.get("实体编码") and m.get("status") == "已合并"]

    def emit_stats_snapshot(self, entry_id: int) -> int:
        """发布完成后向治理看板输黄金记录数快照。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return 0
        stats = store.rows("stats_overview")
        stat_id = max((int(r.get("id", 0)) for r in stats), default=0) + 1
        stats.append({
            "id": stat_id,
            "统计时间": datetime.now(timezone.utc).isoformat(),
            "实体编码": entry.get("实体编码", ""),
            "金记录数": str(entry.get("金记录数", "0")),
            "重复率": "0%",
            "分发延迟": "0",
            "status": "正常",
            "pending": False,
            "abnormal": False,
        })
        return stat_id

    def register_publish_lineage(self, entry_id: int) -> str | None:
        """登记发布批次到分发模块的血缘边。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        edges = store.rows("lineage_graph")
        edge_id = max((int(r.get("id", 0)) for r in edges), default=0) + 1
        edges.append({
            "id": edge_id,
            "血缘边标识": f"edge-pub-{edge_id}",
            "上游节点": entry.get("发布批次", ""),
            "下游节点": "dist_task",
            "变换规则": "版本分发",
            "登记时间": datetime.now(timezone.utc).isoformat(),
            "status": "已登记",
            "pending": False,
            "abnormal": False,
        })
        return f"edge-pub-{edge_id}"
