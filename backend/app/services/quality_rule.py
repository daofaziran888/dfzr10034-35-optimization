"""质量规则领域服务：完整性、准确性、一致性和及时性规则定义与阻断策略。

Pipeline 3: Quality rule definition → monitoring → issue tracking
"""
from __future__ import annotations

import time as _time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class QualityRuleService(BaseModuleService):
    MODULE = "quality_rule"
    ENTITY = "质量检查"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则编号", "目标资产", "检查类型", "阈值下限", "阈值上限", "阻断级别", "采样比例", "最近检查"]
    REQUIRED_FIELDS = ["规则编号", "目标资产", "检查类型"]
    STATUS_ORDER = ["草稿", "监控中", "已阻断", "已暂停"]
    ACTION_RULES = {"启用监控": "监控中", "确认阻断": "已阻断", "暂停规则": "已暂停"}
    NEGATIVE_ACTIONS = ["确认阻断"]

    def define_rule(self, asset_code: str, check_type: str, threshold_low: str,
                    threshold_high: str, severity: str, rule_code: str | None = None) -> dict[str, Any]:
        """定义质量规则（Pipeline 3 入口）。

        rule_code 用于兼容外部质检单编号（如 Q81-完整性）；缺省仍按 QUAL-序号 生成。
        """
        rule_id = store.next_id(self.MODULE)
        rule = {
            "id": rule_id,
            "规则编号": rule_code or f"QUAL-{rule_id:04d}",
            "目标资产": asset_code,
            "检查类型": check_type,
            "阈值下限": threshold_low,
            "阈值上限": threshold_high,
            "阻断级别": severity,
            "采样比例": "100",
            "最近检查": "",
            # 质检报告台账：每次实测值入库一条，旧值即使被替代也保留可追溯归属
            "质检报告": [],
            "status": "监控中",
            "pending": True,
            "abnormal": False,
            "created_at": _time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        store.rows(self.MODULE).append(rule)
        return rule

    def execute_check(self, rule_id: int, current_value: float, *,
                      source_batch: str = "未标注批次", source_label: str = "检查入库",
                      job_code: str | None = None) -> dict[str, Any]:
        """执行质量检查，如果触发阈值则自动生成治理问题（Pipeline 3 核心方法）。

        每次实测值都会写入规则的「质检报告」台账，并标注取自哪个采集批次
        （source_batch）与来源作业（job_code），保证报告里的每个数都可溯源；
        同一规则同一作业已有生效记录时，旧记录标记为已替代但不删除。
        """
        rule = self.get_entry(rule_id)
        if not rule:
            return {"ok": False, "error": "规则不存在"}

        try:
            low = float(rule.get("阈值下限", 0))
            high = float(rule.get("阈值上限", 100))
        except (ValueError, TypeError):
            return {"ok": False, "error": "阈值配置异常"}

        check_type = rule.get("检查类型", "")
        breached = False
        breach_detail = ""

        if check_type in ("完整性", "completeness"):
            breached = current_value < low
            breach_detail = f"当前值{current_value}低于完整性阈值{low}"
        elif check_type in ("准确性", "accuracy"):
            breached = current_value < low
            breach_detail = f"当前值{current_value}低于准确性阈值{low}"
        elif check_type in ("一致性", "consistency"):
            breached = current_value > high
            breach_detail = f"当前值{current_value}超过一致性上限{high}"
        else:
            breached = current_value < low or current_value > high
            breach_detail = f"当前值{current_value}不在[{low}, {high}]范围内"

        # 更新检查时间
        rule["最近检查"] = _time.strftime("%Y-%m-%d %H:%M:%S")

        # 实测值按采集批次入质检报告台账（旧值保留，仅标记为已替代）
        report_entry = self.attach_metric_reading(
            rule, current_value, source_batch=source_batch,
            source_label=source_label, job_code=job_code,
            breach_detail=breach_detail,
        )

        result = {
            "ok": True,
            "rule_id": rule_id,
            "rule_code": rule.get("规则编号"),
            "check_type": check_type,
            "current_value": current_value,
            "source_batch": source_batch,
            "source_label": source_label,
            "job_code": job_code,
            "report_entry": report_entry,
            "threshold_low": low,
            "threshold_high": high,
            "breached": breached,
            "breach_detail": breach_detail,
            "check_time": rule["最近检查"],
        }

        if breached:
            rule["status"] = "已阻断"
            rule["abnormal"] = True

            # Pipeline 3: 触发问题追踪
            issue = self._create_quality_issue(rule, current_value, breach_detail)
            result["issue_created"] = True
            result["issue"] = issue

            store.log_change(
                self.MODULE, rule_id, "质量检查阻断",
                before="监控中", after="已阻断",
                source="pipeline_3", scope="quality_rule->issue_track"
            )
            self.emit_event("quality_breached", {
                "rule_id": rule_id,
                "asset_code": rule.get("目标资产"),
                "issue_id": issue["id"] if issue else None,
            })

        return result

    def _create_quality_issue(self, rule: dict[str, Any], current_value: float, detail: str) -> dict[str, Any]:
        """生成治理问题并写入 issue_track 表。"""
        issue_id = store.next_id("issue_track")
        severity_map = {"阻断": "严重", "警告": "需关注", "提示": "一般"}
        issue = {
            "id": issue_id,
            "问题编号": f"ISSU-{issue_id:04d}",
            "关联资产": rule.get("目标资产", ""),
            "问题类型": f"质量{rule.get('检查类型', '检查')}异常",
            "严重级别": severity_map.get(rule.get("阻断级别", "提示"), "一般"),
            "指派处理": "数据质量管理员",
            "当前状态": "待处理",
            "修复方案": f"当前值{current_value}不满足质量规则{rule.get('规则编号', '')}",
            "截止日期": _time.strftime("%Y-%m-%d", _time.localtime(_time.time() + 86400 * 7)),
            "status": "待确认",
            "pending": True,
            "abnormal": False,
            "created_at": _time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        store.rows("issue_track").append(issue)
        return issue

    def batch_check_assets(self, asset_code: str, metrics: dict[str, float], *,
                           source_batch: str = "未标注批次", source_label: str = "批量检查",
                           job_code: str | None = None,
                           metric_sources: dict[str, str] | None = None) -> list[dict[str, Any]]:
        """对资产批量执行所有关联质量规则检查。

        metric_sources 可按检查类型单独指定采集批次；缺省统一使用 source_batch，
        从而兼容「同一运行窗内各指标来自不同画像批次」的场景。
        """
        results = []
        rules = store.find_by_field(self.MODULE, "目标资产", asset_code)
        for rule in rules:
            metric_key = rule.get("检查类型", "")
            value = metrics.get(metric_key, 0)
            batch = (metric_sources or {}).get(metric_key, source_batch)
            result = self.execute_check(
                rule["id"], value,
                source_batch=batch, source_label=source_label, job_code=job_code,
            )
            results.append(result)
        return results

    def get_rules_by_asset(self, asset_code: str) -> list[dict[str, Any]]:
        """获取资产的所有质量规则。"""
        return store.find_by_field(self.MODULE, "目标资产", asset_code)

    # ── 质检报告台账（按采集批次溯源） ─────────────────────────

    def attach_metric_reading(self, rule: dict[str, Any], value: float, *,
                              source_batch: str, source_label: str,
                              job_code: str | None = None,
                              breach_detail: str = "") -> dict[str, Any]:
        """把一次实测值登记进规则的质检报告台账。

        同一规则、同一作业下已有的生效记录先标记为「已替代」并记录替代关系，
        旧记录本身保留，审核员仍可沿它找到原始采集批次。
        """
        rule.setdefault("质检报告", [])
        now = _time.strftime("%Y-%m-%d %H:%M:%S")
        entry = {
            "entry_id": len(rule["质检报告"]) + 1,
            "规则编号": rule.get("规则编号", ""),
            "检查类型": rule.get("检查类型", ""),
            "目标资产": rule.get("目标资产", ""),
            "实测值": value,
            "采集批次": source_batch,
            "指标来源": source_label,
            "关联作业": job_code or "",
            "状态": "生效",
            "替代了": "",
            "备注": breach_detail,
            "登记时间": now,
        }
        for older in reversed(rule["质检报告"]):
            if (older.get("状态") == "生效"
                    and older.get("检查类型") == entry["检查类型"]
                    and older.get("关联作业") == entry["关联作业"]):
                older["状态"] = "已替代"
                older["替代为"] = source_batch
                entry["替代了"] = older.get("采集批次", "")
                break
        rule["质检报告"].append(entry)
        return entry

    def reconcile_quality_report(self, asset_code: str, job_code: str) -> dict[str, Any]:
        """补跑后按批次核对质量报告：把作业下各指标对齐到最新可用采集批次。

        只对已经存在生效实测值的指标做核对（不补造数据）；被新批次替代的旧值
        保留在台账里并标注「已替代」。返回逐指标的批次归属与派单次数核对结果。
        """
        from app.services.profile_stats import resolve_profile_batch

        rules = store.find_by_field(self.MODULE, "目标资产", asset_code)
        lines: list[dict[str, Any]] = []
        for rule in rules:
            check_type = rule.get("检查类型", "")
            readings = [
                r for r in rule.get("质检报告", [])
                if r.get("关联作业") == job_code and r.get("检查类型") == check_type
            ]
            if not readings:
                lines.append({
                    "规则编号": rule.get("规则编号"),
                    "检查类型": check_type,
                    "状态": "缺指标",
                    "当前生效值": None,
                    "采集批次": "",
                    "历史批次": [],
                })
                continue

            # 批次对齐：画像已有更新批次的同指标时，把报告对齐到新样本。
            # 这是报告层面的核对，不重新派单、不增加派单次数；旧值标记已替代但保留。
            profile = resolve_profile_batch(asset_code, check_type)
            if profile is not None:
                effective = next(
                    (r for r in reversed(readings) if r.get("状态") == "生效"),
                    readings[-1],
                )
                if effective.get("采集批次") != profile["采集批次"]:
                    aligned = self.attach_metric_reading(
                        rule, float(profile["实测值"]),
                        source_batch=profile["采集批次"],
                        source_label=f"补跑批次对齐（取自{profile['采集批次']}，未重新派单）",
                        job_code=job_code,
                    )
                    readings = [
                        r for r in rule.get("质检报告", [])
                        if r.get("关联作业") == job_code and r.get("检查类型") == check_type
                    ]

            latest = next((r for r in reversed(readings) if r.get("状态") == "生效"), readings[-1])
            latest_batch = profile["采集批次"] if profile else ""
            lines.append({
                "规则编号": rule.get("规则编号"),
                "检查类型": check_type,
                "状态": latest.get("状态"),
                "当前生效值": latest.get("实测值"),
                "采集批次": latest.get("采集批次"),
                "画像最新批次": latest_batch,
                "批次是否最新": (not latest_batch) or latest_batch == latest.get("采集批次"),
                "历史批次": [
                    {"采集批次": r.get("采集批次"), "实测值": r.get("实测值"), "状态": r.get("状态")}
                    for r in readings
                ],
            })
        return {
            "作业编号": job_code,
            "目标资产": asset_code,
            "核对时间": _time.strftime("%Y-%m-%d %H:%M:%S"),
            "明细": lines,
        }