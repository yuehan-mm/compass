package com.oppo.cloud.parser.service.job.detector.plugins.spark;

import com.alibaba.fastjson2.JSON;
import com.oppo.cloud.common.constant.AppCategoryEnum;
import com.oppo.cloud.common.domain.eventlog.DetectorResult;
import com.oppo.cloud.common.domain.eventlog.FileScanAbnormal;
import com.oppo.cloud.common.domain.eventlog.SqlScoreAbnormal;
import com.oppo.cloud.common.domain.eventlog.config.SqlScoreConfig;
import com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.DiagnoseContent;
import com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.SqlDiagnoseService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SqlScoreDetector {

    private final SqlScoreConfig sqlScoreConfig;

    public SqlScoreDetector(SqlScoreConfig sqlScoreConfig) {
        this.sqlScoreConfig = sqlScoreConfig;
    }


    public DetectorResult detect(String sqlCommand, String taskName, FileScanAbnormal fileScanAbnormal) {
        log.info("start SqlScoreDetector");
        log.info("SqlScoreDetector : " + JSON.toJSONString(sqlScoreConfig));
        DetectorResult<SqlScoreAbnormal> detectorResult = new DetectorResult<>(AppCategoryEnum.SQL_SCORE_ANOMALY.getCategory(), false);
        SqlScoreAbnormal sqlScoreAbnormal = new SqlScoreAbnormal();
        DiagnoseContent scriptInfo = SqlDiagnoseService.parseScript(sqlCommand, taskName, fileScanAbnormal);

        boolean isAbnormal = scriptInfo.getScore() < sqlScoreConfig.getMinScore();

        sqlScoreAbnormal.setAbnormal(isAbnormal);
        sqlScoreAbnormal.setScoreContent(scriptInfo.getScoreContent());
        sqlScoreAbnormal.setScore(scriptInfo.getScore());
        detectorResult.setData(sqlScoreAbnormal);
        detectorResult.setAbnormal(isAbnormal);
        return detectorResult;
    }
}
