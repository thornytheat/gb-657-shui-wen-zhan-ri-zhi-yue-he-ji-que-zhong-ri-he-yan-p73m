"""原文清除的字段规则：只抹掉输入文件的原始文本，保留规范化计算事实。

批次一旦进入清除流程，任何一次落库（包括清除前就已经开始跑的旧请求）都必须
经过这里，不能让原文重新写回。规则集中定义，存储层与清除编排共用同一份。
"""
from __future__ import annotations

from . import reasons

# 读数行：原始读数、原始单位、备注都来自输入文件
READING_RAW_FIELDS = ("reading_text", "unit_raw", "note")
# 隔离行：原始单元格与说明里可能回显原值
QUARANTINE_RAW_FIELDS = ("raw_value", "detail")
# 对外展示「这里曾经有原文，但已按批次清除」
CLEARED_MARK = "[已清除]"


def scrub_reading(row: dict) -> dict:
    """复制一行读数并清空原始文本；reading_norm/read_at 等计算事实不动。"""
    cleaned = dict(row)
    for field in READING_RAW_FIELDS:
        cleaned[field] = ""
    return cleaned


def safe_detail(code: str | None) -> str:
    """脱敏后的隔离说明：只保留原因码文案，不回显任何原值。"""
    return f"{reasons.label(code)}；原文已清除"


def scrub_quarantine(row: dict) -> dict:
    """复制一行隔离证据并清空原值，行号/原因码/表号/字段仍可用于审计。"""
    cleaned = dict(row)
    cleaned["raw_value"] = ""
    cleaned["detail"] = safe_detail(cleaned.get("code"))
    return cleaned


def reading_raw_present(row: dict) -> bool:
    return any((row.get(field) or "") != "" for field in READING_RAW_FIELDS)


def quarantine_raw_present(row: dict) -> bool:
    return ((row.get("raw_value") or "") != ""
            or (row.get("detail") or "") != "")
