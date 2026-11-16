"""按原批次复算：只读该批次已落库的规范化事实，不再打开任何样例文件。

清除原文之后，读数原始文本已经不可用；复算必须靠 `reading_norm`、UTC 时间、
表计契约快照与电价快照完成。这样样例文件后来被改、被删、被替换成别的批次，
都不会影响本批次的结果。
"""
from __future__ import annotations

import datetime as dt
import json
from decimal import Decimal

from ... import config, errors
from ..m2_contract import contract as contract_mod
from ..m4_intervals import restore as restore_mod
from ..m5_months import allocate as allocate_mod
from . import compare, history, resume


def require_facts(store, batch_id: str) -> None:
    if store.count_rows(batch_id)["readings"] == 0:
        raise errors.Conflict(
            f"批次 {batch_id} 没有已采信读数，无法按原批次复算",
            code="E_NO_FACTS", detail={"batch_id": batch_id})


def _contracts_from_store(store, batch_id: str,
                          ) -> dict[str, contract_mod.MeterContract]:
    """契约快照：批次落库时的容量、倍率、单位与换表事件，不再读 meters.csv。"""
    contracts: dict[str, contract_mod.MeterContract] = {}
    for row in store.load_meters(batch_id):
        replaced_at = row.get("replaced_at")
        contracts[row["meter_id"]] = contract_mod.MeterContract(
            meter_id=row["meter_id"], name=row["name"],
            capacity=row["capacity"], multiplier=row["multiplier"],
            unit=row["unit"], registers=tuple(row["registers"]),
            replaced_at=(dt.datetime.fromisoformat(replaced_at)
                         if replaced_at else None),
            replaced_by=row.get("replaced_by"),
            tariff=row.get("tariff") or "单一制")
    return contracts


def _records(rows: list[dict], read_type: str) -> list[dict]:
    return [{
        "lineno": row["lineno"], "segment": row["segment"],
        "meter_id": row["meter_id"], "register": row["register"],
        "read_at": row["read_at"], "reading": Decimal(str(row["reading_norm"])),
        "read_type": read_type, "flags": list(row.get("flags") or []),
    } for row in rows if row.get("reading_norm") is not None]


def _prices(totals: dict) -> dict[str, Decimal]:
    """电价快照：清除后仍按原批次落库时的电价复算。"""
    raw = totals.get("prices_json")
    if raw:
        try:
            return {key: Decimal(str(value)) for key, value in json.loads(raw).items()}
        except (ValueError, TypeError):
            pass
    return {register: config.DEFAULT_PRICES[register]
            for register in config.REGISTERS}


def run(store, batch_id: str) -> dict:
    batch = resume.require_batch(store, batch_id)
    require_facts(store, batch_id)
    before = store.load_totals(batch_id)
    contracts = _contracts_from_store(store, batch_id)
    valid = _records(store.load_readings(batch_id, status="valid"), "N")
    estimates = _records(store.load_readings(batch_id, status="estimate"), "E")

    intervals = restore_mod.build_intervals(valid, estimates, contracts)
    interval_stats = restore_mod.totals(intervals, contracts)
    prices = _prices(before)
    allocated = allocate_mod.allocate(intervals, contracts, prices)
    quarantine = store.load_quarantine(batch_id)
    month_usage = allocated["stats"]["usage_total"]
    bill_total = sum((bill["amount_yuan"] for bill in allocated["bills"]), Decimal("0"))
    component_total = sum(
        (sum((Decimal(item["amount"]) for item in bill["components"]), Decimal("0"))
         for bill in allocated["bills"]), Decimal("0"))
    reconcile = compare.build(
        raw_lines=int(before.get("raw_lines") or 0),
        valid=len(valid), estimate=len(estimates), quarantine=len(quarantine),
        blank=int(before.get("blank") or 0),
        interval_usage=interval_stats["primary_usage_kwh"], month_usage=month_usage,
        tou_before=allocated["stats"]["tou_residual_before"],
        tou_after=allocated["stats"]["tou_residual_after"],
        tou_adjustments=allocated["stats"]["tou_adjustments"],
        bill_total=bill_total, component_total=component_total,
        bill_count=len(allocated["bills"]))

    totals = dict(before)
    totals.update({
        "interval_usage": interval_stats["primary_usage_kwh"],
        "month_usage": month_usage,
        "bill_amount": bill_total.quantize(config.MONEY_QUANT),
        "interval_count": len(intervals),
        "allocation_count": len(allocated["allocations"]),
        "bill_count": len(allocated["bills"]),
        "month_count": allocated["stats"]["month_count"],
        "tou_adjustments": allocated["stats"]["tou_adjustments"],
        "balanced": "是" if reconcile["all_balanced"] else "否",
        "reconcile_json": json.dumps(reconcile, ensure_ascii=False, default=str),
    })

    manifest_json = store.load_manifest(batch_id) or {}
    if manifest_json:
        manifest_json = json.loads(json.dumps(manifest_json))
        manifest_json["counters"] = history.counters(
            valid=len(valid), estimate=len(estimates), quarantine=len(quarantine),
            blank=int(before.get("blank") or 0), intervals=len(intervals),
            allocations=len(allocated["allocations"]), bills=len(allocated["bills"]))
        manifest_json["accounts"] = [
            {"key": item["key"], "label": item["label"],
             "left": str(item["left"]), "right": str(item["right"]),
             "diff": str(item["diff"]), "balanced": item["balanced"],
             "note": item.get("note", "")} for item in reconcile["accounts"]]
        manifest_json["all_balanced"] = reconcile["all_balanced"]
        manifest_json["recomputed_at"] = history.iso(history.now_utc())
        manifest_json["recompute_source"] = "stored_facts"

    store.save_recompute(batch_id=batch_id, intervals=intervals,
                         allocations=allocated["allocations"],
                         bills=allocated["bills"], totals=totals,
                         manifest_json=manifest_json or None)

    return {
        "status": batch["status"],
        "batch": batch,
        "idempotent_replay": False,
        "recomputed": True,
        "recompute_source": "stored_facts",
        "stages": store.load_stages(batch_id),
        "totals": totals,
        "reconciliation": reconcile,
        "counts": store.count_rows(batch_id),
    }


__all__ = ["run", "require_facts"]
