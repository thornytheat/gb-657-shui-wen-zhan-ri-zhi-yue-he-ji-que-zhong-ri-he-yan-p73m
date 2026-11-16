"""原文清除编排：请求 → 落审计 → 抹原文 → 置完成；中断后可继续。

三条边界：
- 只对单个批次生效，别的批次的原文与结果不受影响；
- 清除与重试同批互斥（`locks.batch_lock`），清除完成后的旧请求写不回来
  （`Store.save_run` 在写事务里复查状态）；
- 抹原文与审计状态在同一个事务里提交，进程在中间退出时停在 clearing，
  重启或再次请求都能续做，不重复新增审计条目。
"""
from __future__ import annotations

from ... import errors
from . import history, locks, recompute, resume

STATUS_CLEARING = "clearing"
STATUS_CLEARED = "cleared"


def request_clear(store, batch_id: str, *, operator: str = "",
                  reason: str = "") -> dict:
    """清除入口：可重复调用，已清除的批次返回现有审计而不新增记录。"""
    with locks.batch_lock(batch_id):
        batch = resume.require_batch(store, batch_id)
        state = store.redaction_state(batch_id)
        if state == STATUS_CLEARED:
            return _summary(store, batch_id, replay=True)
        if state is None:
            # 没有已采信事实的批次（失败批次）本来就没有原文可清，
            # 允许清除会让它再也不能按样例重试，直接拒绝。
            recompute.require_facts(store, batch_id)
        stamp = history.iso(history.now_utc())
        store.begin_redaction(batch_id=batch_id,
                              operator=(operator or "数据员").strip() or "数据员",
                              reason=(reason or "隔离行误填联系方式").strip(),
                              stamp=stamp)
        resume_clear(store, batch_id)
        return _summary(store, batch_id, replay=state == STATUS_CLEARING)


def resume_clear(store, batch_id: str) -> dict | None:
    """从 clearing 状态继续：重复执行幂等，原文已空也算完成。"""
    with locks.batch_lock(batch_id):
        state = store.redaction_state(batch_id)
        if state is None:
            return None
        if state == STATUS_CLEARED:
            return _summary(store, batch_id, replay=True)
        stamp = history.iso(history.now_utc())
        counts = store.apply_redaction(batch_id, stamp=stamp)
        # 清完按原批次已落库事实复算一次：四账仍旧取自规范化数据
        if store.count_rows(batch_id)["readings"]:
            recompute.run(store, batch_id)
        return _summary(store, batch_id, replay=False, counts=counts)


def resume_pending(store) -> list[dict]:
    """启动时把上次中断留下的 clearing 收尾，返回处理过的批次。"""
    done: list[dict] = []
    for row in store.pending_redactions():
        try:
            summary = resume_clear(store, row["batch_id"])
        except errors.MeterbackError:
            continue
        if summary is not None:
            done.append(summary)
    return done


def audit(store, batch_id: str) -> dict:
    resume.require_batch(store, batch_id)
    return {
        "batch_id": batch_id,
        "state": store.redaction_state(batch_id) or "none",
        "events": [_event_payload(item) for item in store.list_redactions(batch_id)],
    }


def _summary(store, batch_id: str, *, replay: bool,
             counts: dict | None = None) -> dict:
    batch = store.get_batch(batch_id) or {}
    events = store.list_redactions(batch_id)
    latest = events[0] if events else {}
    return {
        "batch_id": batch_id,
        "state": store.redaction_state(batch_id) or "none",
        "redacted_at": batch.get("redacted_at"),
        "replay": replay,
        "counts": counts if counts is not None else (latest.get("counts") or {}),
        "audit": _event_payload(latest) if latest else None,
    }


def _event_payload(item: dict) -> dict:
    return {
        "redaction_id": item["redaction_id"],
        "status": item["status"],
        "requested_at": item["requested_at"],
        "completed_at": item.get("completed_at"),
        "operator": item["operator"],
        "reason": item["reason"],
        "sample": item["sample"],
        "fields": item.get("fields") or {},
        "counts": item.get("counts") or {},
    }
