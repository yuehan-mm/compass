package com.oppo.cloud.parser.service.job.detector.plugins.spark;

import com.oppo.cloud.common.constant.AppCategoryEnum;
import com.oppo.cloud.common.domain.eventlog.DetectorResult;
import com.oppo.cloud.common.domain.eventlog.FileScanAbnormal;
import com.oppo.cloud.common.domain.eventlog.SqlScoreAbnormal;
import com.oppo.cloud.parser.domain.job.TaskParam;
import com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.service.SqlDiagnoseService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JobPerfDetector {

    public DetectorResult detect(TaskParam taskParam, FileScanAbnormal fileScanAbnormal) {
        log.debug("start JobPerfDetector");
        DetectorResult<SqlScoreAbnormal> detectorResult = new DetectorResult<>(AppCategoryEnum.JOB_PERF_ANOMALY.getCategory(), false);
        SqlScoreAbnormal sqlScoreAbnormal = SqlDiagnoseService.buildSqlScoreAbnormal(taskParam.getTaskApp(), fileScanAbnormal);
        detectorResult.setData(sqlScoreAbnormal);
        detectorResult.setAbnormal(sqlScoreAbnormal.getAbnormal());
        return detectorResult;
    }
}
