"""原文清除：批次隔离、规范化事实保留、复算不读样例、旧请求不回写、中断续做。

这些用例固定住四条底线：
- 清除后页面、接口、下载都拿不到原始文本；
- 处理量、行数与四账逐项不变；
- 复算只用已落库事实，样例被改/被删也不影响；
- 清除与重试并发时按批次串行，重跑不新增记录。
"""
from __future__ import annotations

import json
from dataclasses import replace
from decimal import Decimal

import pytest

from app import errors, redact
from app.modules.m6_batches import clear, recompute, service
from app.store import Store
from fixtures import expected

# 只在原始单元格里出现、规范化之后不会保留的片段
RAW_PIECES = ("1,212.5", "３０５０", "３，０５０", "无功", "10500.0", "abc",
              "-5.0", "丢了前导零", "全角抄见", "同键 A", "同键 B", "多一列")


def _run(settings, store):
    return service.run(settings, store, sample="readings_dirty.csv")


def test_clear_removes_raw_text_but_keeps_facts(settings, store):
    result = _run(settings, store)
    batch_id = result["batch"]["batch_id"]
    before_counts = store.count_rows(batch_id)
    before_totals = {k: v for k, v in store.load_totals(batch_id).items()}

    assert any(row["raw_value"] for row in store.load_quarantine(batch_id))
    assert any(row["reading_text"] for row in store.load_readings(batch_id))

    out = clear.request_clear(store, batch_id, operator="数据员",
                              reason="隔离行误填联系方式")
    assert out["state"] == "cleared"
    assert out["audit"]["status"] == "done"
    assert out["counts"] == {"readings_cleared": 46, "quarantine_cleared": 14}

    # 原文没了
    assert all(row["reading_text"] == "" and row["unit_raw"] == ""
               and row["note"] == "" for row in store.load_readings(batch_id))
    assert all(row["raw_value"] == "" for row in store.load_quarantine(batch_id))
    # 规范化事实与四账没变
    assert store.count_rows(batch_id) == before_counts
    after_totals = store.load_totals(batch_id)
    for key in ("raw_lines", "valid", "estimate", "quarantine", "blank",
                "interval_usage", "month_usage", "bill_amount"):
        assert after_totals[key] == before_totals[key], key
    assert after_totals["interval_usage"] == str(expected.TOTAL_USAGE)
    assert after_totals["bill_amount"] == str(expected.TOTAL_AMOUNT)
    assert after_totals["balanced"] == "是"
    # 隔离审计仍能按行号/原因码核对，只是不再带原值
    first = store.load_quarantine(batch_id)[0]
    assert first["lineno"] > 1 and first["code"].startswith("Q_")
    assert first["detail"].endswith("原文已清除")


def test_clear_is_idempotent_and_audited_once(settings, store):
    batch_id = _run(settings, store)["batch"]["batch_id"]
    first = clear.request_clear(store, batch_id)
    second = clear.request_clear(store, batch_id)
    assert second["replay"] is True
    assert len(store.list_redactions(batch_id)) == 1
    assert second["audit"]["redaction_id"] == first["audit"]["redaction_id"]


def test_clear_scoped_to_one_batch(settings, store):
    dirty = _run(settings, store)["batch"]["batch_id"]
    other = service.run(settings, store,
                        sample="readings_truncated.csv")["batch"]["batch_id"]
    assert dirty != other
    clear.request_clear(store, dirty)
    assert store.redaction_state(other) is None
    assert all(row["reading_text"] for row in store.load_readings(other))
    assert any(row["raw_value"] for row in store.load_quarantine(other))


def test_recompute_reads_no_sample_and_keeps_all_accounts(settings, store, tmp_path):
    result = _run(settings, store)
    batch_id = result["batch"]["batch_id"]
    before = result["totals"]
    clear.request_clear(store, batch_id)

    # 复算时把样例目录换成一个空目录：任何读样例的路径都会 404/FileNotFound
    empty = tmp_path / "empty_samples"
    empty.mkdir()
    settings = replace(settings, sample_dir=str(empty))
    again = recompute.run(store, batch_id)
    assert again["recompute_source"] == "stored_facts"
    assert again["reconciliation"]["all_balanced"] is True
    assert again["totals"]["interval_usage"] == before["interval_usage"]
    assert again["totals"]["month_usage"] == before["month_usage"]
    assert again["totals"]["bill_amount"] == before["bill_amount"]
    assert again["counts"] == result["counts"]


def test_retry_after_clear_uses_frozen_facts_and_adds_no_rows(settings, store,
                                                              tmp_path):
    batch_id = _run(settings, store)["batch"]["batch_id"]
    clear.request_clear(store, batch_id)
    counts = store.count_rows(batch_id)
    totals = store.load_totals(batch_id)
    events = len(store.list_redactions(batch_id))

    empty = tmp_path / "empty_samples"
    empty.mkdir()
    settings = replace(settings, sample_dir=str(empty))
    result = service.retry(settings, store, batch_id)

    assert result["recompute_source"] == "stored_facts"
    assert result["batch"]["batch_id"] == batch_id
    assert store.count_rows(batch_id) == counts
    assert store.load_totals(batch_id) == totals
    assert len(store.list_redactions(batch_id)) == events


def test_run_on_cleared_batch_does_not_require_sample_file(settings, store,
                                                           tmp_path):
    batch_id = _run(settings, store)["batch"]["batch_id"]
    clear.request_clear(store, batch_id)
    counts = store.count_rows(batch_id)
    sample_dir = tmp_path / "renamed_samples"
    sample_dir.mkdir()
    settings = replace(settings, sample_dir=str(sample_dir))
    payload = service.run(settings, store, sample="readings_dirty.csv")
    assert payload["batch"]["batch_id"] == batch_id
    assert payload["recompute_source"] == "stored_facts"
    assert store.count_rows(batch_id) == counts


def test_stale_run_cannot_write_raw_back(settings, store):
    """旧请求晚于清除落库：写事务里复查状态，原文被拦下且不报错。"""
    batch_id = _run(settings, store)["batch"]["batch_id"]
    clear.request_clear(store, batch_id)
    stale_readings = [{
        "lineno": 2, "segment": 1, "meter_id": "M0001", "register": "PEAK",
        "read_at": "2026-01-01T00:00:00+00:00", "reading_text": "640.0",
        "unit_raw": "kWh", "reading_norm": Decimal("640.0"), "unit_norm": "kWh",
        "read_type": "N", "status": "valid", "code": None, "flags": [],
        "note": "联系人 13800000000",
    }]
    stale_quarantine = [{
        "lineno": 7, "segment": 1, "code": "Q_BAD_REGISTER", "stage": "contract",
        "level": "error", "field": "register", "raw_value": "无功",
        "meter_id": "M0001", "detail": "寄存器无法识别：'无功'",
    }]
    blocked = store.save_run(
        batch={"batch_id": batch_id, "batch_key": "k", "run_id": "MB-stale",
               "sample": "readings_dirty.csv", "status": "done",
               "created_at": "2026-01-01T00:00:00+00:00",
               "finished_at": "2026-01-01T00:00:00+00:00",
               "input_digest": "d", "rule_version": settings.rule_version,
               "params": {}, "note": ""},
        meters=[], readings=stale_readings, intervals=[], allocations=[],
        bills=[], quarantine=stale_quarantine, stages=[],
        totals={}, manifest_json={})
    assert blocked is True
    assert store.load_readings(batch_id)[0]["reading_text"] == ""
    assert store.load_readings(batch_id)[0]["note"] == ""
    assert store.load_quarantine(batch_id)[0]["raw_value"] == ""
    assert store.redaction_state(batch_id) == "cleared"


def test_interrupted_clear_resumes_without_duplicate_audit(settings, store, app):
    batch_id = _run(settings, store)["batch"]["batch_id"]
    # 模拟清除刚开始、进程就退出：审计停在 clearing
    store.begin_redaction(batch_id=batch_id, operator="数据员",
                          reason="隔离行误填联系方式",
                          stamp="2026-01-01T00:00:00+00:00")
    assert store.redaction_state(batch_id) == "clearing"
    assert recompute.require_facts(store, batch_id) is None

    resumed = clear.resume_pending(store)
    assert [item["batch_id"] for item in resumed] == [batch_id]
    assert store.redaction_state(batch_id) == "cleared"
    assert len(store.list_redactions(batch_id)) == 1
    assert all(row["reading_text"] == "" for row in store.load_readings(batch_id))


def test_clear_and_retry_concurrency_serializes_per_batch(settings, store):
    import threading

    batch_id = _run(settings, store)["batch"]["batch_id"]
    other_id = service.run(settings, store,
                           sample="readings_truncated.csv")["batch"]["batch_id"]
    before = store.count_rows(batch_id)
    errors_seen: list[BaseException] = []

    def do_clear():
        try:
            clear.request_clear(store, batch_id)
        except BaseException as exc:  # noqa: BLE001
            errors_seen.append(exc)

    def do_retry():
        try:
            service.retry(settings, store, batch_id)
        except BaseException as exc:  # noqa: BLE001
            errors_seen.append(exc)

    threads = [threading.Thread(target=do_clear), threading.Thread(target=do_retry)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    assert not errors_seen
    assert store.redaction_state(batch_id) == "cleared"
    assert store.count_rows(batch_id) == before
    # 另一个批次不被波及
    assert store.redaction_state(other_id) is None
    assert all(row["reading_text"] for row in store.load_readings(other_id))


def test_compute_without_facts_is_rejected_for_empty_batch(settings, store):
    store.save_attempt(
        batch={"batch_id": "mb-empty", "batch_key": "k", "run_id": "MB-e",
               "sample": "readings_empty.csv", "status": "failed",
               "created_at": "2026-01-01T00:00:00+00:00",
               "finished_at": None, "input_digest": "d",
               "rule_version": settings.rule_version, "params": {}, "note": ""},
        stages=[])
    with pytest.raises(errors.Conflict) as excinfo:
        recompute.run(store, "mb-empty")
    assert excinfo.value.code == "E_NO_FACTS"


def test_api_clear_hides_raw_everywhere(client_with_run):
    client = client_with_run
    batch = client.get("/api/batches").json()["batches"][0]
    batch_id = batch["batch_id"]

    response = client.post(f"/api/batches/{batch_id}/redact",
                           json={"operator": "数据员", "reason": "隔离行误填联系方式"})
    assert response.status_code == 200
    assert response.json()["state"] == "cleared"

    quarantine = client.get(f"/api/quarantine?batch_id={batch_id}").json()
    assert all(row["raw_value"] == "" for row in quarantine["rows"])
    assert all("原文已清除" in row["detail"] for row in quarantine["rows"])

    readings = client.get(f"/api/readings/{batch_id}").json()
    assert readings["readings"]
    for path in ("/api/export/readings.csv", "/api/export/quarantine.csv"):
        body = client.get(f"{path}?batch_id={batch_id}").text
        for piece in RAW_PIECES:
            assert piece not in body, (path, piece)

    page = client.get("/batches")
    assert page.status_code == 200
    assert "已清除" in page.text


def test_api_retry_after_clear_and_rerun_stay_on_same_batch(client_with_run):
    client = client_with_run
    batch_id = client.get("/api/batches").json()["batches"][0]["batch_id"]
    client.post(f"/api/batches/{batch_id}/redact", json={})
    before = client.get("/api/reconcile").json()

    retried = client.post(f"/api/batches/{batch_id}/retry", json={}).json()
    assert retried["recompute_source"] == "stored_facts"
    after = client.get("/api/reconcile").json()
    assert after["accounts"] == before["accounts"]
    assert after["all_balanced"] is True

    rerun = client.post("/api/runs", json={"sample": "readings_dirty.csv"}).json()
    assert rerun["batch_id"] == batch_id
    assert rerun["recompute_source"] == "stored_facts"
    batches = client.get("/api/batches").json()["batches"]
    assert len(batches) == 1
    assert len(client.get(f"/api/batches/{batch_id}/redaction").json()["events"]) == 1


def test_redaction_manifest_records_frozen_recompute(client_with_run):
    client = client_with_run
    batch_id = client.get("/api/batches").json()["batches"][0]["batch_id"]
    client.post(f"/api/batches/{batch_id}/redact", json={})
    payload = json.loads(client.get(
        f"/api/export/manifest.json?batch_id={batch_id}").text)
    assert payload["manifest"]["recompute_source"] == "stored_facts"
    assert payload["manifest"]["all_balanced"] is True
    assert payload["counts"]["quarantine"] == expected.QUARANTINE


def test_redact_module_keeps_identity_fields():
    """脱敏只动原始文本三列，表号/寄存器/时间/规范化读数不动。"""
    row = {"lineno": 3, "segment": 1, "meter_id": "007", "register": "TOTAL",
           "read_at": "2026-02-15T00:00:00+00:00", "reading_text": "210.0",
           "unit_raw": "kWh", "reading_norm": Decimal("210.0"),
           "unit_norm": "kWh", "read_type": "N", "status": "valid",
           "code": None, "flags": [], "note": "联系人 138"}
    cleaned = redact.scrub_reading(row)
    assert cleaned["reading_text"] == cleaned["unit_raw"] == cleaned["note"] == ""
    for key in ("meter_id", "register", "read_at", "reading_norm", "status"):
        assert cleaned[key] == row[key]
    assert row["reading_text"] == "210.0"   # 不改原对象


def test_store_redaction_survives_new_store_instance(settings, store):
    batch_id = _run(settings, store)["batch"]["batch_id"]
    clear.request_clear(store, batch_id)
    reopened = Store(settings.db_path)
    assert reopened.redaction_state(batch_id) == "cleared"
    assert all(row["reading_text"] == "" for row in reopened.load_readings(batch_id))
    assert len(reopened.list_redactions(batch_id)) == 1


def test_app_startup_finishes_interrupted_clear(settings, store):
    from app.main import create_app
    batch_id = _run(settings, store)["batch"]["batch_id"]
    # 进程在「已登记 clearing、还没抹原文」时退出
    store.begin_redaction(batch_id=batch_id, operator="数据员", reason="隔离行误填联系方式",
                          stamp="2026-01-01T00:00:00+00:00")
    create_app(settings)
    reopened = Store(settings.db_path)
    assert reopened.redaction_state(batch_id) == "cleared"
    assert all(row["reading_text"] == "" for row in reopened.load_readings(batch_id))
    assert all(row["raw_value"] == "" for row in reopened.load_quarantine(batch_id))
    assert len(reopened.list_redactions(batch_id)) == 1
    # 再次启动不重复
    create_app(settings)
    assert len(reopened.list_redactions(batch_id)) == 1


def test_quarantine_csv_route_has_no_raw_after_clear(client_with_run):
    client = client_with_run
    batch_id = client.get("/api/batches").json()["batches"][0]["batch_id"]
    client.post(f"/api/batches/{batch_id}/redact", json={})
    body = client.get(f"/api/quarantine.csv?batch_id={batch_id}").text
    for piece in RAW_PIECES:
        assert piece not in body, piece
    assert "原文已清除" in body
