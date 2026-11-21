"""质量调度失败续做测试：运行窗、回执防重、采集批次溯源与补跑核对。

场景（对应需求描述）：
- 质检单 Q81 含完整性、准确性两个测点；首次运行窗只有完整性实测 97，准确性缺指标；
- 同一运行窗之后到达的画像批次携带更新样本（完整性 99、准确性 93）；
- 中断后续做只检查仍待确认的项目，重复回执不可再次派单，缺指标须标出来源；
- 旧结果 97 即使被 99 替代也保留可追溯归属，报告里每个数都能指出采集批次；
- 完整性先完成、准确性稍后才到，补跑后按批次核对质量报告与派单次数。
"""
from __future__ import annotations

import app.store as store_mod
from app.services.profile_stats import profile_batch_of, resolve_profile_batch
from app.services.quality_rule import QualityRuleService
from app.services.profile_stats import ProfileStatsService
from app.services.schedule_job import ITEM_DONE, ITEM_MISSING, ScheduleJobService


def _fresh_services():
    store_mod.store.reset()
    return QualityRuleService(), ProfileStatsService(), ScheduleJobService()


def _seed_q81_rules(quality: QualityRuleService, asset: str = "ASSET-Q81"):
    """登记质检单 Q81 的两个测点：完整性先到位、准确性稍后才到。"""
    completeness = quality.define_rule(
        asset, "完整性", "95", "100", "阻断", rule_code="Q81-完整性")
    accuracy = quality.define_rule(
        asset, "准确性", "90", "100", "警告", rule_code="Q81-准确性")
    return completeness, accuracy


def _schedule_quality_job(schedule: ScheduleJobService, asset: str):
    job = schedule.schedule_task(asset, "质量检查", "0 6 * * *")
    return job


class TestQualityScheduleRerun:

    def test_first_run_missing_metric_is_marked_with_source(self):
        quality, profile, schedule = _fresh_services()
        asset = "ASSET-Q81"
        _seed_q81_rules(quality, asset)
        job = _schedule_quality_job(schedule, asset)

        # 首个画像批次只有完整性 97，没有准确性
        profile.profile_dataset(
            asset, [{"id": "1"}, {"id": "2"}],
            batch_id="BATCH-OLD-01",
            quality_metrics={"完整性": 97.0},
        )

        result = schedule.execute_job(job["id"])
        assert result["ok"] is True
        job_after = schedule.get_entry(job["id"])
        assert job_after["status"] == "失败待补"

        details = {d["检查类型"]: d for d in job_after["补跑明细"]}
        # 完整性已完成且带批次溯源
        assert details["完整性"]["状态"] == ITEM_DONE
        assert details["完整性"]["实测值"] == 97.0
        assert details["完整性"]["采集批次"] == "BATCH-OLD-01"
        assert details["完整性"]["回执编号"]
        # 准确性缺指标：不派单、无回执，并标出来源
        assert details["准确性"]["状态"] == ITEM_MISSING
        assert details["准确性"]["回执编号"] == ""
        assert details["准确性"]["派单次数"] == 0
        assert "准确性" in details["准确性"]["缺指标来源"]

        summary = job_after["调度汇总"]
        assert summary["测点总数"] == 2
        assert summary["已完成"] == 1
        assert summary["缺指标"] == 1
        assert summary["派单回执总数"] == 1

    def test_rerun_only_checks_pending_and_keeps_old_receipt(self):
        quality, profile, schedule = _fresh_services()
        asset = "ASSET-Q81"
        _seed_q81_rules(quality, asset)
        job = _schedule_quality_job(schedule, asset)

        profile.profile_dataset(
            asset, [{"id": "1"}], batch_id="BATCH-OLD-01",
            quality_metrics={"完整性": 97.0})
        schedule.execute_job(job["id"])

        first_receipt = next(
            d["回执编号"] for d in schedule.get_entry(job["id"])["补跑明细"]
            if d["检查类型"] == "完整性")

        # 新画像批次：完整性更新样本 99，准确性 93（两项分别 99 和 93）
        profile.profile_dataset(
            asset, [{"id": "1"}, {"id": "2"}, {"id": "3"}],
            batch_id="BATCH-NEW-02",
            quality_metrics={"完整性": 99.0, "准确性": 93.0},
        )

        rerun = schedule.rerun_failed_checks(job["id"])
        assert rerun["ok"] is True
        # 续做只派了准确性这一个待补测点
        assert len(rerun["rerun"]) == 1
        assert rerun["rerun"][0]["检查类型"] == "准确性"
        assert rerun["rerun"][0]["采集批次"] == "BATCH-NEW-02"
        assert rerun["rerun"][0]["实测值"] == 93.0

        job_after = schedule.get_entry(job["id"])
        details = {d["检查类型"]: d for d in job_after["补跑明细"]}
        # 完整性旧回执复用、没有再次派单
        assert details["完整性"]["回执编号"] == first_receipt
        assert details["完整性"]["派单次数"] == 1
        # 准确性在新批次补齐
        assert details["准确性"]["状态"] == ITEM_DONE
        assert details["准确性"]["派单次数"] == 1
        assert details["准确性"]["采集批次"] == "BATCH-NEW-02"

        summary = job_after["调度汇总"]
        assert summary["已完成"] == 2
        assert summary["缺指标"] == 0
        assert summary["派单回执总数"] == 2
        assert summary["回执复用次数"] == 1
        assert job_after["status"] == "已完成"

    def test_duplicate_receipt_cannot_be_dispatched_again(self):
        quality, profile, schedule = _fresh_services()
        asset = "ASSET-Q81-DUP"
        _seed_q81_rules(quality, asset)
        job = _schedule_quality_job(schedule, asset)

        profile.profile_dataset(
            asset, [{"id": "1"}], batch_id="BATCH-OLD-01",
            quality_metrics={"完整性": 97.0})
        schedule.execute_job(job["id"])

        # 新批次把准确性补齐，续做后作业全部完成
        profile.profile_dataset(
            asset, [{"id": "1"}], batch_id="BATCH-NEW-02",
            quality_metrics={"完整性": 99.0, "准确性": 93.0})
        schedule.rerun_failed_checks(job["id"])
        assert schedule.get_entry(job["id"])["status"] == "已完成"

        receipts_before = schedule.get_entry(job["id"])["调度汇总"]["派单回执总数"]
        assert receipts_before == 2

        # 再次续做：没有待补测点，重复回执不可再次派单，零新增回执
        again = schedule.rerun_failed_checks(job["id"])
        assert again["rerun"] == []
        assert "无待补测点" in again["message"]
        receipts_after = schedule.get_entry(job["id"])["调度汇总"]["派单回执总数"]
        assert receipts_after == receipts_before
        for item in schedule.get_entry(job["id"])["补跑明细"]:
            assert item["派单次数"] == 1

    def test_rerun_without_new_profile_keeps_missing_source(self):
        quality, profile, schedule = _fresh_services()
        asset = "ASSET-Q81-WAIT"
        _seed_q81_rules(quality, asset)
        job = _schedule_quality_job(schedule, asset)

        profile.profile_dataset(
            asset, [{"id": "1"}], batch_id="BATCH-OLD-01",
            quality_metrics={"完整性": 97.0})
        schedule.execute_job(job["id"])

        # 没有新画像批次就续做：准确性仍缺指标、仍不派单，并继续标来源
        rerun = schedule.rerun_failed_checks(job["id"])
        assert len(rerun["rerun"]) == 1
        item = rerun["rerun"][0]
        assert item["检查类型"] == "准确性"
        assert item["状态"] == ITEM_MISSING
        assert item["回执编号"] == ""
        assert item["缺指标来源"]
        assert schedule.get_entry(job["id"])["status"] == "失败待补"

    def test_report_keeps_superseded_97_with_batch_attribution(self):
        quality, profile, schedule = _fresh_services()
        asset = "ASSET-Q81-TRACE"
        completeness, _ = _seed_q81_rules(quality, asset)
        job = _schedule_quality_job(schedule, asset)

        profile.profile_dataset(
            asset, [{"id": "1"}], batch_id="BATCH-OLD-01",
            quality_metrics={"完整性": 97.0})
        schedule.execute_job(job["id"])

        profile.profile_dataset(
            asset, [{"id": "1"}, {"id": "2"}], batch_id="BATCH-NEW-02",
            quality_metrics={"完整性": 99.0, "准确性": 93.0})
        schedule.rerun_failed_checks(job["id"])

        rule = quality.get_entry(completeness["id"])
        report = rule["质检报告"]
        # 台账里同时保留 97（旧批次）与 99（新批次），97 被替代但不删除
        values = {(r["实测值"], r["采集批次"], r["状态"]) for r in report}
        assert (97.0, "BATCH-OLD-01", "已替代") in values
        assert (99.0, "BATCH-NEW-02", "生效") in values
        old = next(r for r in report if r["实测值"] == 97.0)
        new = next(r for r in report if r["实测值"] == 99.0)
        assert old["替代为"] == "BATCH-NEW-02"
        assert new["替代了"] == "BATCH-OLD-01"
        # 每个数都关联到作业与采集批次，审核员可逐数溯源
        for r in report:
            assert r["关联作业"] == job["作业编号"]
            assert r["采集批次"]

    def test_reconcile_report_lists_every_number_by_batch(self):
        quality, profile, schedule = _fresh_services()
        asset = "ASSET-Q81-RECON"
        _seed_q81_rules(quality, asset)
        job = _schedule_quality_job(schedule, asset)

        profile.profile_dataset(
            asset, [{"id": "1"}], batch_id="BATCH-OLD-01",
            quality_metrics={"完整性": 97.0})
        schedule.execute_job(job["id"])
        profile.profile_dataset(
            asset, [{"id": "1"}], batch_id="BATCH-NEW-02",
            quality_metrics={"完整性": 99.0, "准确性": 93.0})
        rerun = schedule.rerun_failed_checks(job["id"])

        lines = {line["检查类型"]: line for line in rerun["report"]["明细"]}
        assert lines["完整性"]["当前生效值"] == 99.0
        assert lines["完整性"]["采集批次"] == "BATCH-NEW-02"
        # 历史批次完整保留，97 的归属仍可追溯
        history = {h["采集批次"]: h for h in lines["完整性"]["历史批次"]}
        assert history["BATCH-OLD-01"]["实测值"] == 97.0
        assert history["BATCH-OLD-01"]["状态"] == "已替代"
        assert lines["准确性"]["当前生效值"] == 93.0
        assert lines["准确性"]["采集批次"] == "BATCH-NEW-02"

    def test_resolve_metric_uses_latest_profile_batch_per_metric(self):
        quality, profile, schedule = _fresh_services()
        asset = "ASSET-Q81-LATEST"
        _seed_q81_rules(quality, asset)

        profile.profile_dataset(
            asset, [{"id": "1"}], batch_id="BATCH-OLD-01",
            quality_metrics={"完整性": 97.0})
        profile.profile_dataset(
            asset, [{"id": "1"}], batch_id="BATCH-NEW-02",
            quality_metrics={"完整性": 99.0, "准确性": 93.0})

        located = resolve_profile_batch(asset, "完整性")
        assert located["采集批次"] == "BATCH-NEW-02"
        assert located["实测值"] == 99.0
        accuracy = resolve_profile_batch(asset, "准确性")
        assert accuracy["采集批次"] == "BATCH-NEW-02"
        assert resolve_profile_batch(asset, "一致性") is None

    def test_legacy_profile_without_batch_field_is_supported(self):
        """输入标识兼容已有画像批次：没有采集批次字段的旧画像按画像编号兜底。"""
        quality, profile, schedule = _fresh_services()
        asset = "ASSET-LEGACY"
        _seed_q81_rules(quality, asset)
        job = _schedule_quality_job(schedule, asset)

        # 模拟历史画像：无「采集批次」字段，但带质量指标
        legacy = {
            "id": 77, "画像编号": "PROF-0077", "目标资产": asset,
            "质量指标": {"完整性": 97.0}, "status": "可用",
        }
        store_mod.store.rows("profile_stats").append(legacy)
        assert profile_batch_of(legacy) == "PROF-0077"

        result = schedule.execute_job(job["id"])
        details = {d["检查类型"]: d for d in result["results"]}
        assert details["完整性"]["采集批次"] == "PROF-0077"
        assert details["完整性"]["实测值"] == 97.0

    def test_completeness_finishes_before_accuracy_arrives(self):
        """同一资产完整性先完成、准确性稍后才到的分批到达顺序。"""
        quality, profile, schedule = _fresh_services()
        asset = "ASSET-Q81-ORDER"
        _seed_q81_rules(quality, asset)
        job = _schedule_quality_job(schedule, asset)

        # 只有完整性
        profile.profile_dataset(
            asset, [{"id": "1"}], batch_id="BATCH-OLD-01",
            quality_metrics={"完整性": 97.0})
        first = schedule.execute_job(job["id"])
        first_details = {d["检查类型"]: d for d in first["results"]}
        assert first_details["完整性"]["状态"] == ITEM_DONE
        assert first_details["准确性"]["状态"] == ITEM_MISSING

        # 中间一次续做，准确性仍未到 -> 继续缺指标，不派单
        waiting = schedule.rerun_failed_checks(job["id"])
        assert waiting["rerun"][0]["状态"] == ITEM_MISSING
        assert waiting["summary"]["派单回执总数"] == 1

        # 准确性批次到达后续做完成
        profile.profile_dataset(
            asset, [{"id": "1"}], batch_id="BATCH-NEW-02",
            quality_metrics={"准确性": 93.0})
        done = schedule.rerun_failed_checks(job["id"])
        assert done["summary"]["已完成"] == 2
        assert done["summary"]["派单回执总数"] == 2
        assert schedule.get_entry(job["id"])["status"] == "已完成"

        # 全流程派单次数：完整性 1 次、准确性 1 次，无重复派单
        for item in schedule.get_entry(job["id"])["补跑明细"]:
            assert item["派单次数"] == 1
