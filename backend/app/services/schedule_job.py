"""调度治理领域服务：治理任务的定时调度编排、依赖检查、失败续做和失败告警。

编排自动化治理任务：质量检查、合规扫描、生命周期流转的定时执行。

质量调度失败续做模型
--------------------
- 作业每次执行形成一个「运行窗」，窗内逐测点（质量规则检查类型）派单并生成回执；
- 实测值从数据画像的采集批次解析，回执与质检报告都记录指标取自哪个批次；
- 缺指标的测点不派单，标记「缺指标」并注明来源（待画像批次补齐）；
- 中断后续做只检查仍「待确认」的项目；同一测点的回执不可再次派单（回执防重）；
- 已完成的旧结果即使被更新批次替代，也保留在质检报告台账里可追溯。
"""
from __future__ import annotations

import time as _time
from typing import Any

from app.services.base import BaseModuleService
from app.services.profile_stats import profile_batch_of, resolve_profile_batch
from app.store import store

# 待确认 / 已完成 / 缺指标 三种补跑明细状态
ITEM_PENDING = "待确认"
ITEM_DONE = "已完成"
ITEM_MISSING = "缺指标"


class ScheduleJobService(BaseModuleService):
    MODULE = "schedule_job"
    ENTITY = "治理调度"
    OPERATION_TERM = "调度"
    LIST_FIELDS = ["作业编号", "治理类型", "目标资产", "Cron表达式", "依赖检查",
                   "超时上限", "失败通知", "下次执行", "运行窗口", "补跑状态"]
    REQUIRED_FIELDS = ["作业编号", "治理类型", "目标资产"]
    STATUS_ORDER = ["待激活", "执行中", "已完成", "失败待补", "已停用"]
    ACTION_RULES = {"激活任务": "执行中", "立即执行": "已完成",
                    "失败续做": "已完成", "停用任务": "已停用"}
    NEGATIVE_ACTIONS = ["停用任务"]

    def schedule_task(self, asset_code: str, job_type: str, cron_expr: str,
                      timeout: str = "3600", notify_on_failure: str = "是") -> dict[str, Any]:
        """创建治理调度任务。"""
        job_id = store.next_id(self.MODULE)
        job = {
            "id": job_id,
            "作业编号": f"SCHE-{job_id:04d}",
            "治理类型": job_type,
            "目标资产": asset_code,
            "Cron表达式": cron_expr,
            "依赖检查": self._compute_dependencies(job_type, asset_code),
            "超时上限": timeout,
            "失败通知": notify_on_failure,
            "下次执行": _time.strftime("%Y-%m-%d %H:%M:%S", _time.localtime(_time.time() + 3600)),
            "运行窗口": "",
            "补跑状态": "未执行",
            # 调度补跑检查明细：每个测点一条，贯穿首次执行与各次续做
            "补跑明细": [],
            # 作业级汇总：随检查结果更新
            "调度汇总": {},
            "status": "待激活",
            "pending": True,
            "abnormal": False,
            "created_at": _time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        store.rows(self.MODULE).append(job)
        return job

    def _compute_dependencies(self, job_type: str, asset_code: str) -> str:
        """计算调度作业的依赖。"""
        deps = []
        if job_type in ("质量检查", "quality_check"):
            rules = store.find_by_field("quality_rule", "目标资产", asset_code)
            if rules:
                deps.append(f"依赖{len(rules)}条质量规则")
        if job_type in ("合规扫描", "compliance_scan"):
            contracts = store.find_by_field("data_contract", "数据资产", asset_code)
            if contracts:
                deps.append(f"依赖{len(contracts)}个数据契约")
        if job_type in ("生命周期", "lifecycle"):
            policies = store.find_by_field("lifecycle_policy", "目标资产", asset_code)
            if policies:
                deps.append(f"依赖{len(policies)}条生命周期策略")
        return "; ".join(deps) if deps else "无"

    # ── 测点解析：作业送入规则的指标从哪个画像批次来 ─────────────

    def _resolve_metric(self, asset_code: str, check_type: str) -> dict[str, Any]:
        """沿调用链定位测点：返回指标值与采集批次，缺指标时标出来源。"""
        located = resolve_profile_batch(asset_code, check_type)
        if located is None:
            return {
                "available": False,
                "value": None,
                "采集批次": "",
                "来源说明": f"画像批次尚未提供{check_type}指标，等待新采集批次补齐",
            }
        return {
            "available": True,
            "value": located["实测值"],
            "采集批次": located["采集批次"],
            "画像编号": located["画像编号"],
            "来源说明": f"取自画像采集批次{located['采集批次']}",
        }

    def _new_receipt_no(self, job: dict[str, Any]) -> str:
        """生成作业内唯一回执编号。"""
        job["_receipt_seq"] = int(job.get("_receipt_seq", 0)) + 1
        return f"RCPT-{job['作业编号']}-{job['_receipt_seq']:03d}"

    def _run_quality_item(self, job: dict[str, Any], rule: dict[str, Any],
                          window_id: str) -> dict[str, Any]:
        """对单个测点派单执行（含回执防重），返回补跑明细条目。"""
        from app.services import QUALITY_SERVICE

        check_type = rule.get("检查类型", "")
        metric = self._resolve_metric(job["目标资产"], check_type)
        now = _time.strftime("%Y-%m-%d %H:%M:%S")

        item = {
            "规则编号": rule.get("规则编号"),
            "rule_id": rule["id"],
            "检查类型": check_type,
            "运行窗口": window_id,
            "状态": ITEM_PENDING,
            "回执编号": "",
            "派单次数": 0,
            "实测值": None,
            "采集批次": "",
            "指标来源": "",
            "缺指标来源": "",
            "最近更新": now,
        }

        if not metric["available"]:
            # 缺指标不派单：标出来源，等待后续画像批次
            item["状态"] = ITEM_MISSING
            item["缺指标来源"] = metric["来源说明"]
            return item

        # 有指标才派单：生成回执并执行检查，回执编号一经生成不可重复派单
        receipt_no = self._new_receipt_no(job)
        result = QUALITY_SERVICE.execute_check(
            rule["id"], float(metric["value"]),
            source_batch=metric["采集批次"],
            source_label=metric["来源说明"],
            job_code=job["作业编号"],
        )
        item.update({
            "状态": ITEM_DONE,
            "回执编号": receipt_no,
            "派单次数": 1,
            "实测值": metric["value"],
            "采集批次": metric["采集批次"],
            "指标来源": metric["来源说明"],
            "是否阻断": bool(result.get("breached")),
            "breach_detail": result.get("breach_detail", ""),
        })
        return item

    def _refresh_summary(self, job: dict[str, Any]) -> dict[str, Any]:
        """调度汇总随检查结果更新。"""
        details = job.get("补跑明细", [])
        summary = {
            "作业编号": job["作业编号"],
            "运行窗口": job.get("运行窗口", ""),
            "测点总数": len(details),
            "已完成": sum(1 for d in details if d["状态"] == ITEM_DONE),
            "待确认": sum(1 for d in details if d["状态"] == ITEM_PENDING),
            "缺指标": sum(1 for d in details if d["状态"] == ITEM_MISSING),
            "派单回执总数": sum(int(d.get("派单次数", 0)) for d in details),
            "重复派单拦截次数": int(job.get("_duplicate_blocked", 0)),
            "回执复用次数": int(job.get("_receipt_reused", 0)),
            "更新时间": _time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        job["调度汇总"] = summary
        if summary["待确认"] or summary["缺指标"]:
            job["补跑状态"] = f"待补{summary['待确认'] + summary['缺指标']}项"
        else:
            job["补跑状态"] = "全部完成"
        return summary

    def execute_job(self, job_id: int) -> dict[str, Any]:
        """执行调度任务：质量检查按测点派单，缺指标保留为失败待补。"""
        job = self.get_entry(job_id)
        if not job:
            return {"ok": False, "error": "作业不存在"}

        job["status"] = "执行中"
        job["pending"] = True
        job["执行时间"] = _time.strftime("%Y-%m-%d %H:%M:%S")

        job_type = job.get("治理类型", "")
        asset = job.get("目标资产", "")
        window_id = f"WIN-{job['作业编号']}-{_time.strftime('%Y%m%d%H%M%S')}"
        job["运行窗口"] = window_id
        results: list[dict[str, Any]] = []

        if "质量" in job_type:
            from app.services import QUALITY_SERVICE
            rules = QUALITY_SERVICE.get_rules_by_asset(asset)
            details = [self._run_quality_item(job, rule, window_id) for rule in rules]
            job["补跑明细"] = details
            results = [
                {k: v for k, v in item.items() if k != "rule_id"} for item in details
            ]
            summary = self._refresh_summary(job)
            # 有任一测点未完成即进入失败待补，等待续做
            job["status"] = "失败待补" if (summary["待确认"] or summary["缺指标"]) else "已完成"
            job["pending"] = job["status"] == "失败待补"
            job["abnormal"] = job["status"] == "失败待补"
        elif "合规" in job_type:
            from app.services import COMPLIANCE_SERVICE
            scan = COMPLIANCE_SERVICE.scan_asset(asset, "自动调度合规检查",
                                                  ["分类标签复核", "脱敏规则完备性", "审计异常"])
            results = [scan]
            job["status"] = "已完成"
            job["pending"] = False
            self._refresh_summary(job)
        elif "生命" in job_type:
            policies = store.find_by_field("lifecycle_policy", "目标资产", asset)
            from app.services import LIFECYCLE_SERVICE
            for policy in policies:
                result = LIFECYCLE_SERVICE.transition_stage(policy["id"], "活跃阶段")
                results.append(result)
            job["status"] = "已完成"
            job["pending"] = False
            self._refresh_summary(job)

        job["results"] = results

        self.emit_event("job_completed", {
            "job_id": job_id,
            "job_type": job_type,
            "asset": asset,
            "results_count": len(results),
            "run_window": window_id,
            "job_status": job["status"],
        })

        return {
            "ok": True,
            "job_id": job_id,
            "job_type": job_type,
            "run_window": window_id,
            "results": results,
            "summary": job.get("调度汇总", {}),
        }

    # ── 失败续做 ──────────────────────────────────────────────

    def rerun_failed_checks(self, job_id: int) -> dict[str, Any]:
        """中断后的续做：只检查仍待确认/缺指标的测点，回执不可再次派单。

        - 已完成且持有回执的测点直接跳过（回执复用，不重复派单）；
        - 待确认测点若已存在回执，拒绝再次派单并计入重复派单拦截；
        - 缺指标测点重新解析画像批次，指标到位才派单，否则继续标注来源；
        - 续做后按批次核对质量报告，并刷新派单次数等调度汇总。
        """
        job = self.get_entry(job_id)
        if not job:
            return {"ok": False, "error": "作业不存在"}

        window_id = f"WIN-{job['作业编号']}-R{_time.strftime('%Y%m%d%H%M%S')}"
        job["运行窗口"] = window_id
        details = job.setdefault("补跑明细", [])

        if not any(d["状态"] in (ITEM_PENDING, ITEM_MISSING) for d in details):
            # 没有失败待补项目：不允许再次派单
            summary = self._refresh_summary(job)
            return {
                "ok": True,
                "message": "无待补测点，未生成新回执",
                "run_window": window_id,
                "rerun": [],
                "summary": summary,
                "report": None,
            }

        from app.services import QUALITY_SERVICE

        rerun_items: list[dict[str, Any]] = []
        for item in details:
            if item["状态"] == ITEM_DONE:
                # 旧结果保留：回执复用，绝不重复派单
                if item.get("回执编号"):
                    job["_receipt_reused"] = int(job.get("_receipt_reused", 0)) + 1
                continue

            if item["状态"] == ITEM_PENDING and item.get("回执编号"):
                # 重复的回执不可再次派单
                job["_duplicate_blocked"] = int(job.get("_duplicate_blocked", 0)) + 1
                item["拦截说明"] = f"回执{item['回执编号']}已派单，禁止重复派单"
                continue

            rule = store.find("quality_rule", item["rule_id"])
            if rule is None:
                continue
            metric = self._resolve_metric(job["目标资产"], item["检查类型"])
            now = _time.strftime("%Y-%m-%d %H:%M:%S")
            item["运行窗口"] = window_id
            item["最近更新"] = now

            if not metric["available"]:
                # 仍缺指标：继续标出来源，不派单
                item["状态"] = ITEM_MISSING
                item["缺指标来源"] = metric["来源说明"]
                rerun_items.append({k: v for k, v in item.items() if k != "rule_id"})
                continue

            if item.get("回执编号"):
                # 理论上待确认且带回执的分支已在上面拦截，这里再兜底防重
                job["_duplicate_blocked"] = int(job.get("_duplicate_blocked", 0)) + 1
                continue

            receipt_no = self._new_receipt_no(job)
            result = QUALITY_SERVICE.execute_check(
                rule["id"], float(metric["value"]),
                source_batch=metric["采集批次"],
                source_label=metric["来源说明"],
                job_code=job["作业编号"],
            )
            item.update({
                "状态": ITEM_DONE,
                "回执编号": receipt_no,
                "派单次数": int(item.get("派单次数", 0)) + 1,
                "实测值": metric["value"],
                "采集批次": metric["采集批次"],
                "指标来源": metric["来源说明"],
                "缺指标来源": "",
                "是否阻断": bool(result.get("breached")),
                "breach_detail": result.get("breach_detail", ""),
            })
            rerun_items.append({k: v for k, v in item.items() if k != "rule_id"})

        # 续做后按批次核对质量报告与派单次数
        report = QUALITY_SERVICE.reconcile_quality_report(job["目标资产"], job["作业编号"])
        summary = self._refresh_summary(job)

        if summary["待确认"] or summary["缺指标"]:
            job["status"] = "失败待补"
            job["pending"] = True
            job["abnormal"] = True
        else:
            job["status"] = "已完成"
            job["pending"] = False
            job["abnormal"] = False

        store.log_change(
            self.MODULE, job["id"], "质量调度失败续做",
            before="失败待补", after=job["status"],
            source="schedule_rerun",
            scope=f"schedule_job->quality_rule@window={window_id}",
        )
        self.emit_event("job_rerun", {
            "job_id": job["id"],
            "run_window": window_id,
            "rerun_count": len(rerun_items),
            "summary": summary,
        })

        return {
            "ok": True,
            "run_window": window_id,
            "rerun": rerun_items,
            "summary": summary,
            "report": report,
        }

    def get_rerun_details(self, job_id: int) -> dict[str, Any]:
        """查看调度补跑检查明细与汇总，供审核员逐数核对采集批次。"""
        job = self.get_entry(job_id)
        if not job:
            return {"ok": False, "error": "作业不存在"}
        return {
            "ok": True,
            "作业编号": job["作业编号"],
            "运行窗口": job.get("运行窗口", ""),
            "补跑状态": job.get("补跑状态", ""),
            "明细": [
                {k: v for k, v in item.items() if k != "rule_id"}
                for item in job.get("补跑明细", [])
            ],
            "调度汇总": job.get("调度汇总", {}),
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """动作分派：失败续做走专门的续做链路，其余动作沿用通用状态流转。"""
        if action == "失败续做":
            result = self.rerun_failed_checks(entry_id)
            if not result.get("ok"):
                return None, result.get("error", "续做失败")
            entry = self.get_entry(entry_id)
            return entry, "失败续做已完成，补跑明细与调度汇总已按批次更新"
        return super().run_action(entry_id, action)
