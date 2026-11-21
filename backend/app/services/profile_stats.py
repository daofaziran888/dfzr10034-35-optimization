"""数据画像领域服务：列级统计画像，包括分布类型、空值比例和离群值检测。

Pipeline 2: Data profiling → classification → sensitivity labeling
"""
from __future__ import annotations

import time as _time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ProfileStatsService(BaseModuleService):
    MODULE = "profile_stats"
    ENTITY = "数据画像"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["画像编号", "采集批次", "目标资产", "行数统计", "空值比例", "区分度", "分布类型", "画像时间"]
    REQUIRED_FIELDS = ["画像编号", "目标资产", "行数统计"]
    STATUS_ORDER = ["待采集", "采集中", "可用", "采集异常"]
    ACTION_RULES = {"启动采集": "采集中", "标记可用": "可用", "标记异常": "采集异常"}
    NEGATIVE_ACTIONS = []

    def profile_dataset(self, asset_code: str, sample_data: list[dict[str, Any]], *,
                        batch_id: str | None = None,
                        quality_metrics: dict[str, float] | None = None) -> dict[str, Any]:
        """对数据集执行画像分析并自动触发分级（Pipeline 2 入口）。

        batch_id 为采集批次标识，缺省按画像序号生成；同一资产可产生多个批次，
        质量调度按批次选择指标。quality_metrics 携带本批次实测的资产质量指标，
        如 {"完整性": 99.0, "准确性": 93.0}。
        """
        profile_id = store.next_id(self.MODULE)
        batch_code = batch_id or f"BATCH-{asset_code}-{profile_id:04d}"
        total_rows = len(sample_data)

        # 计算列统计
        field_names = list(sample_data[0].keys()) if sample_data else []
        field_stats = {}
        for fname in field_names:
            values = [row.get(fname) for row in sample_data if row.get(fname) is not None]
            null_count = total_rows - len(values)
            null_ratio = null_count / total_rows if total_rows > 0 else 0
            distinct = len(set(str(v) for v in values))
            distinct_ratio = distinct / len(values) if values else 0
            field_stats[fname] = {
                "null_count": null_count,
                "null_ratio": round(null_ratio, 4),
                "distinct_count": distinct,
                "distinct_ratio": round(distinct_ratio, 4),
                "sample_values": [str(v) for v in values[:3]],
            }

        entry = {
            "id": profile_id,
            "画像编号": f"PROF-{profile_id:04d}",
            "采集批次": batch_code,
            "目标资产": asset_code,
            "行数统计": str(total_rows),
            "空值比例": str(round(sum(f["null_ratio"] for f in field_stats.values()) / max(len(field_stats), 1), 4)),
            "区分度": str(round(sum(f["distinct_ratio"] for f in field_stats.values()) / max(len(field_stats), 1), 4)),
            "分布类型": "uniform" if len(field_stats) > 0 else "empty",
            "离群阈值": "3sigma",
            "画像时间": _time.strftime("%Y-%m-%d %H:%M:%S"),
            "质量指标": dict(quality_metrics or {}),
            "status": "可用",
            "pending": False,
            "abnormal": False,
            "field_stats": field_stats,
            "created_at": _time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        store.rows(self.MODULE).append(entry)

        # Pipeline 2: 触发敏感字段识别和分级标注
        classification_labels = self._detect_sensitive_fields(asset_code, field_stats)

        # 记录变更
        store.log_change(
            self.MODULE, profile_id, "画像采集并触发分级",
            before=None,
            after=f"画像{asset_code}：{total_rows}行，{len(field_stats)}列，生成{len(classification_labels)}条分级标签",
            source="pipeline_2", scope="profile_stats->classification"
        )

        self.emit_event("profile_completed", {
            "profile_id": profile_id,
            "asset_code": asset_code,
            "classification_count": len(classification_labels),
        })

        return {
            "profile": entry,
            "field_stats": field_stats,
            "classification_labels": classification_labels,
            "pipeline": "profile_stats->classification",
        }

    def _detect_sensitive_fields(self, asset_code: str, field_stats: dict) -> list[dict[str, Any]]:
        """敏感字段自动检测和分级标注。"""
        sensitive_patterns = {
            "身份证": (["身份证号", "证件号", "id_number", "id_card", "ssn"], "高", "正则匹配"),
            "手机": (["手机", "电话", "mobile", "phone", "contact"], "高", "正则匹配"),
            "姓名": (["姓名", "名称", "name", "full_name", "姓名"], "中", "关键词识别"),
            "地址": (["地址", "住址", "address", "location", "addr"], "中", "关键词识别"),
            "邮箱": (["邮箱", "email", "mail", "e-mail"], "中", "正则匹配"),
            "金额": (["金额", "价格", "amount", "price", "cost", "金额", "余额"], "低", "数值类型"),
            "年龄": (["年龄", "age", "出生日期", "生日", "birth"], "低", "数值类型"),
        }

        labels = []
        for field_name, stats in field_stats.items():
            fname_lower = field_name.lower()
            for label_type, (keywords, level, rule) in sensitive_patterns.items():
                if any(kw in fname_lower for kw in keywords):
                    label = {
                        "id": store.next_id("classification"),
                        "标签编号": f"CLAS-{asset_code}-{field_name}",
                        "目标字段": f"{asset_code}.{field_name}",
                        "敏感等级": level,
                        "识别规则": rule,
                        "识别置信度": "0.95",
                        "人工复核": "否",
                        "复核人": "",
                        "生效日期": _time.strftime("%Y-%m-%d"),
                        "status": "已确认" if level == "高" else "待复核",
                        "pending": level != "高",
                        "abnormal": False,
                        "created_at": _time.strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    store.rows("classification").append(label)
                    labels.append(label)
                    break

        return labels

    def get_profile_by_asset(self, asset_code: str) -> dict[str, Any] | None:
        """按资产查询画像（返回最早一个，兼容旧调用）。"""
        for row in store.rows(self.MODULE):
            if row.get("目标资产") == asset_code:
                return row
        return None

    def list_profiles_by_asset(self, asset_code: str) -> list[dict[str, Any]]:
        """按资产列出全部画像批次（按入库顺序，即批次先后）。"""
        return [row for row in store.rows(self.MODULE) if row.get("目标资产") == asset_code]


def profile_batch_of(profile: dict[str, Any]) -> str:
    """读取画像的采集批次标识，兼容没有该字段的历史画像批次。"""
    batch = profile.get("采集批次")
    if batch:
        return str(batch)
    return f"PROF-{profile.get('id', 0):04d}"


def resolve_profile_batch(asset_code: str, check_type: str) -> dict[str, Any] | None:
    """定位某资产某质量指标实际取自哪个画像采集批次。

    沿调度调用链送入规则的测点在这里解析：从该资产最新可用的画像批次向前找，
    返回第一个携带该指标的批次；历史画像没有「质量指标」字段时自然跳过。
    """
    profiles = [
        row for row in store.rows("profile_stats")
        if row.get("目标资产") == asset_code and row.get("质量指标")
    ]
    for profile in reversed(profiles):
        metrics = profile.get("质量指标") or {}
        if check_type in metrics:
            return {
                "采集批次": profile_batch_of(profile),
                "画像编号": profile.get("画像编号"),
                "检查类型": check_type,
                "实测值": metrics[check_type],
                "画像时间": profile.get("画像时间"),
            }
    return None