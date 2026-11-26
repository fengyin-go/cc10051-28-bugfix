"""回滚终态登记簿：为每个发布版本保存唯一一份撤回确认依据。

设计要点：
- 撤回确认按「目标版本」去重，同一版本只会有一份确认依据（回执编号 rbr-<版本>）。
  同一回执重复到达时返回同一份登记，不新建、不改写；
- 取消只是追加一份取消回执（rbc-<回滚任务>），不会抹掉已经登记的撤回回执；
- 执行失败另行登记失败回执（rbf-<回滚任务>），与取消终结互不覆盖；
- 发布端、分发端都以这里的终态登记判断版本是否仍可领取。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.store import store

CONFIRM_TABLE = "rollback_confirmations"
CANCEL_TABLE = "rollback_cancellations"
FAILURE_TABLE = "rollback_failures"
RECEIPT_TABLE = "dist_receipt"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_version(value: Any) -> str:
    return str(value or "").strip()


def confirmation_code(version: str) -> str:
    return f"rbr-{normalize_version(version)}" if normalize_version(version) else ""


def cancel_code(task: dict[str, Any]) -> str:
    identity = str(task.get("回滚任务", "")).strip() or str(task.get("id", ""))
    return f"rbc-{identity}" if identity else ""


def failure_code(task: dict[str, Any]) -> str:
    identity = str(task.get("回滚任务", "")).strip() or str(task.get("id", ""))
    return f"rbf-{identity}" if identity else ""


def _next_id(module: str) -> int:
    return max((int(row.get("id", 0)) for row in store.rows(module)), default=0) + 1


def find_confirmation(version: str) -> dict[str, Any] | None:
    """按目标版本读取撤回确认依据；不存在返回 None。"""
    version = normalize_version(version)
    if not version:
        return None
    return next(
        (row for row in store.rows(CONFIRM_TABLE) if row.get("目标版本") == version),
        None,
    )


def is_withdrawn(version: str) -> bool:
    """版本是否已被确认撤回（以终态确认依据为准）。"""
    if find_confirmation(version) is not None:
        return True
    # 兼容历史数据：发布行已经落到「已回滚」也算已撤回，避免旧版本被重新领取。
    return any(
        normalize_version(row.get("版本标识")) == normalize_version(version)
        and row.get("status") == "已回滚"
        for row in store.rows("golden_publish")
    )


def withdrawn_versions(entity_code: str | None = None) -> list[str]:
    """输出已被确认撤回的版本清单，可按实体编码过滤。"""
    versions: list[str] = []
    for row in store.rows(CONFIRM_TABLE):
        version = normalize_version(row.get("目标版本"))
        if not version:
            continue
        if entity_code and entity_code != str(row.get("实体编码", "")):
            continue
        if version not in versions:
            versions.append(version)
    for row in store.rows("golden_publish"):
        if row.get("status") != "已回滚":
            continue
        version = normalize_version(row.get("版本标识"))
        if entity_code and entity_code != str(row.get("实体编码", "")):
            continue
        if version and version not in versions:
            versions.append(version)
    return versions


def _mirror_to_receipt_center(row: dict[str, Any], reason: str) -> dict[str, Any]:
    """把回滚终态回执投递到分发回执中心；同编号回执只登记一次且内容不变。"""
    code = str(row.get("回执编号", ""))
    receipts = store.rows(RECEIPT_TABLE)
    existing = next((item for item in receipts if item.get("回执编号") == code), None)
    if existing is not None:
        return existing
    receipt = {
        "id": _next_id(RECEIPT_TABLE),
        "回执编号": code,
        "分发批次": f"rollback-{row.get('目标版本', '')}",
        "订阅方": ",".join(row.get("订阅方", []) or []),
        "回执状态": "已回收",
        "差异说明": reason,
        "status": "已回收",
        "pending": False,
        "abnormal": False,
        "终态": row.get("终态", ""),
        "目标版本": row.get("目标版本", ""),
    }
    receipts.append(receipt)
    return receipt


def register_confirmation(
    task: dict[str, Any],
    *,
    subscribers: list[str] | None = None,
) -> dict[str, Any]:
    """登记（或返回既有）撤回确认依据，保证一个版本只有一份确认。"""
    version = normalize_version(task.get("目标版本"))
    existing = find_confirmation(version)
    if existing is not None:
        return existing
    record = {
        "id": _next_id(CONFIRM_TABLE),
        "回执编号": confirmation_code(version),
        "目标版本": version,
        "回滚任务": task.get("回滚任务", ""),
        "回滚任务ID": task.get("id"),
        "实体编码": task.get("实体编码", ""),
        "终态": "已生效",
        "登记时间": _now(),
        "订阅方": list(subscribers or []),
        "status": "已生效",
        "pending": False,
        "abnormal": False,
    }
    store.rows(CONFIRM_TABLE).append(record)
    mirrored = _mirror_to_receipt_center(
        record, f"撤回 {version}，该版本后续不可再领取或分发给订阅方"
    )
    record["回执中心记录"] = mirrored.get("id")
    return record


def register_cancel(task: dict[str, Any], reason: str) -> dict[str, Any]:
    """登记取消回执；重复取消返回同一份回执，且不影响撤回确认。"""
    code = cancel_code(task)
    existing = next(
        (row for row in store.rows(CANCEL_TABLE) if row.get("回执编号") == code),
        None,
    )
    if existing is not None:
        return existing
    record = {
        "id": _next_id(CANCEL_TABLE),
        "回执编号": code,
        "回滚任务": task.get("回滚任务", ""),
        "回滚任务ID": task.get("id"),
        "目标版本": normalize_version(task.get("目标版本")),
        "终态": "已取消",
        "取消原因": reason,
        "登记时间": _now(),
        "status": "已取消",
        "pending": False,
        "abnormal": False,
    }
    store.rows(CANCEL_TABLE).append(record)
    _mirror_to_receipt_center(
        record,
        f"取消回滚任务 {record.get('回滚任务', '')}：目标版本未被撤回，撤回回执保持不变",
    )
    return record


def find_cancel(task: dict[str, Any]) -> dict[str, Any] | None:
    code = cancel_code(task)
    return next(
        (row for row in store.rows(CANCEL_TABLE) if row.get("回执编号") == code),
        None,
    )


def register_failure(task: dict[str, Any], reason: str) -> dict[str, Any]:
    """登记执行失败回执；与取消终结分开，避免两类终态混在一起。"""
    code = failure_code(task)
    existing = next(
        (row for row in store.rows(FAILURE_TABLE) if row.get("回执编号") == code),
        None,
    )
    if existing is not None:
        return existing
    record = {
        "id": _next_id(FAILURE_TABLE),
        "回执编号": code,
        "回滚任务": task.get("回滚任务", ""),
        "回滚任务ID": task.get("id"),
        "目标版本": normalize_version(task.get("目标版本")),
        "终态": "回滚失败",
        "失败原因": reason,
        "登记时间": _now(),
        "status": "回滚失败",
        "pending": False,
        "abnormal": True,
    }
    store.rows(FAILURE_TABLE).append(record)
    _mirror_to_receipt_center(record, f"回滚执行失败：{reason}")
    return record


def find_failure(task: dict[str, Any]) -> dict[str, Any] | None:
    code = failure_code(task)
    return next(
        (row for row in store.rows(FAILURE_TABLE) if row.get("回执编号") == code),
        None,
    )
