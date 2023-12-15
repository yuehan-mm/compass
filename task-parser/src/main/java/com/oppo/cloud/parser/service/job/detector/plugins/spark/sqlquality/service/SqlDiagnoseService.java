package com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.service;


import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.oppo.cloud.common.constant.AppCategoryEnum;
import com.oppo.cloud.common.domain.elasticsearch.TaskApp;
import com.oppo.cloud.common.domain.eventlog.DetectorResult;
import com.oppo.cloud.common.domain.eventlog.FileScanAbnormal;
import com.oppo.cloud.common.domain.eventlog.SpeculativeMapReduceAbnormal;
import com.oppo.cloud.common.domain.eventlog.SqlScoreAbnormal;
import com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.util.HttpRequestUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.util.Const.*;

@Slf4j
public class SqlDiagnoseService {

    public static SqlScoreAbnormal buildSqlScoreAbnormal(TaskApp taskApp, FileScanAbnormal fileScanAbnormal) {
        LinkedHashMap<String, DiagnoseDesc> res = new LinkedHashMap<>();
        BigDecimal score = BigDecimal.valueOf(100);
        try {

            // 扫描文件数量
            FileScanAbnormal.FileScanReport fileScanReport = fileScanAbnormal.getScriptReport();
            if (fileScanReport != null) {
                if (fileScanReport.getTotalFileCount() > SQL_SCAN_FILE_COUNT_THRESHOLD) {
                    res.put("SQL_SCAN_FILE_COUNT", new DiagnoseDesc("SQL_SCAN_FILE_COUNT", SQL_SCAN_FILE_COUNT_NAME,
                            SQL_SCAN_FILE_COUNT_THRESHOLD, fileScanReport.getTotalFileCount(),
                            BigDecimal.valueOf((fileScanReport.getTotalFileCount() - SQL_SCAN_FILE_COUNT_THRESHOLD))
                                    .multiply(BigDecimal.valueOf(SQL_SCAN_FILE_COUNT_SCORE)).doubleValue(),
                            SQL_SCAN_FILE_COUNT_DESC));
                } else {
                    res.put("SQL_SCAN_FILE_COUNT", new DiagnoseDesc("SQL_SCAN_FILE_COUNT", SQL_SCAN_FILE_COUNT_NAME,
                            SQL_SCAN_FILE_COUNT_THRESHOLD, fileScanReport.getTotalFileCount(),
                            0, SQL_SCAN_FILE_COUNT_DESC));
                }
                // 扫描文件大小
                if (fileScanReport.getTotalFileSize() > SQL_SCAN_FILE_SIZE_THRESHOLD) {
                    res.put("SQL_SCAN_FILE_SIZE", new DiagnoseDesc("SQL_SCAN_FILE_SIZE", SQL_SCAN_FILE_SIZE_NAME,
                            SQL_SCAN_FILE_SIZE_THRESHOLD, fileScanReport.getTotalFileSize(),
                            BigDecimal.valueOf(Math.ceil((fileScanReport.getTotalFileSize() - SQL_SCAN_FILE_SIZE_THRESHOLD) / (1024 * 1024 * 100.0)))
                                    .multiply(BigDecimal.valueOf(SQL_SCAN_FILE_SIZE_SCORE)).doubleValue(),
                            SQL_SCAN_FILE_SIZE_DESC));
                } else {
                    res.put("SQL_SCAN_FILE_SIZE", new DiagnoseDesc("SQL_SCAN_FILE_SIZE", SQL_SCAN_FILE_SIZE_NAME,
                            SQL_SCAN_FILE_SIZE_THRESHOLD, fileScanReport.getTotalFileSize(),
                            0, SQL_SCAN_FILE_SIZE_DESC));
                }
                // 扫描小文件数量
                if (fileScanReport.getSmallFileCount() > SQL_SCAN_SMALL_FILE_COUNT_THRESHOLD) {
                    res.put("SQL_SCAN_SMALL_FILE_COUNT", new DiagnoseDesc("SQL_SCAN_SMALL_FILE_COUNT", SQL_SCAN_SMALL_FILE_COUNT_NAME,
                            SQL_SCAN_SMALL_FILE_COUNT_THRESHOLD, fileScanReport.getSmallFileCount(),
                            BigDecimal.valueOf((fileScanReport.getSmallFileCount() - SQL_SCAN_SMALL_FILE_COUNT_THRESHOLD))
                                    .multiply(BigDecimal.valueOf(SQL_SCAN_SMALL_FILE_COUNT_SCORE)).doubleValue(),
                            SQL_SCAN_SMALL_FILE_COUNT_DESC));
                } else {
                    res.put("SQL_SCAN_SMALL_FILE_COUNT", new DiagnoseDesc("SQL_SCAN_SMALL_FILE_COUNT", SQL_SCAN_SMALL_FILE_COUNT_NAME,
                            SQL_SCAN_SMALL_FILE_COUNT_THRESHOLD, fileScanReport.getSmallFileCount(),
                            0, SQL_SCAN_SMALL_FILE_COUNT_DESC));
                }

                // 扫描分区数量
                if (fileScanReport.getPartitionCount() > SQL_SCAN_PARTITION_COUNT_THRESHOLD) {
                    res.put("SQL_SCAN_PARTITION_COUNT", new DiagnoseDesc("SQL_SCAN_PARTITION_COUNT", SQL_SCAN_PARTITION_COUNT_NAME,
                            SQL_SCAN_PARTITION_COUNT_THRESHOLD, fileScanReport.getPartitionCount(),
                            BigDecimal.valueOf((fileScanReport.getPartitionCount() - SQL_SCAN_PARTITION_COUNT_THRESHOLD))
                                    .multiply(BigDecimal.valueOf(SQL_SCAN_PARTITION_COUNT_SCORE)).doubleValue(),
                            SQL_SCAN_PARTITION_COUNT_DESC));
                } else {
                    res.put("SQL_SCAN_PARTITION_COUNT", new DiagnoseDesc("SQL_SCAN_PARTITION_COUNT", SQL_SCAN_PARTITION_COUNT_NAME,
                            SQL_SCAN_PARTITION_COUNT_THRESHOLD, fileScanReport.getPartitionCount(),
                            0, SQL_SCAN_PARTITION_COUNT_DESC));
                }
            }

            score = score.subtract(res.values().stream().map(x -> BigDecimal.valueOf(x.deductScore)).reduce((x, y) -> x.add(y)).orElse(BigDecimal.valueOf(0)));
        } catch (Exception e) {
            log.error("buildSqlScoreAbnormal fail. TaskName: " + taskApp.getTaskName() + "\tmsg: " + e.getMessage());
        }

        SqlScoreAbnormal diagnoseContent = new SqlScoreAbnormal();
        diagnoseContent.setDiagnoseResult(JSON.toJSONString(res));
        diagnoseContent.setScore(score.doubleValue());
        diagnoseContent.setAbnormal(score.doubleValue() < 60);
        return diagnoseContent;
    }


    public static String getGrammarDiagnoseResult(String command, TaskApp taskApp) {
        try {
            Map<String, Object> body = new HashMap<>();
            body.put("action", "insert");
            body.put("command", command);
            body.put("scriptType", "hive");
            String jsonStr = HttpRequestUtils.doPost(REQUEST_URL, JSONObject.toJSONString(body));
            JSONObject json = JSONObject.parseObject(jsonStr);
            if (json.getInteger("status") != 200) throw new RuntimeException(jsonStr);
            return json.getJSONObject("data").getString("diagnoseResult");
        } catch (Exception e) {
            log.error("get getGrammarDiagnoseResult fail. TaskName:" + taskApp.getTaskName() + "\tmsg:" + e.getMessage());
            log.error("consume log record error : {}", e);
        }
        return null;
    }

    public static SqlScoreAbnormal buildMapReduceJobPerfAbnormal(TaskApp taskApp, List<DetectorResult> dataList) {
        LinkedHashMap<String, DiagnoseDesc> res = new LinkedHashMap<>();
        BigDecimal score = BigDecimal.valueOf(100);
        try {
            Map<String, DetectorResult> detectorResultMap = new HashMap<>();
            for (DetectorResult detectorResult : dataList) {
                detectorResultMap.put(detectorResult.getAppCategory(), detectorResult);
            }

            if (detectorResultMap.containsKey(AppCategoryEnum.SPECULATIVE_MAP_REDUCE.getCategory())) {
                SpeculativeMapReduceAbnormal speculativeMapReduceAbnormal = (SpeculativeMapReduceAbnormal) detectorResultMap
                        .get(AppCategoryEnum.SPECULATIVE_MAP_REDUCE.getCategory()).getData();

                // map 数量过多
                if (speculativeMapReduceAbnormal.getFinishedMaps() > MAPREDUCE_MAP_TASK_COUNT_THRESHOLD) {
                    res.put("MAPREDUCE_MAP_TASK_COUNT", new DiagnoseDesc("MAPREDUCE_MAP_TASK_COUNT", MAPREDUCE_MAP_TASK_COUNT_NAME,
                            MAPREDUCE_MAP_TASK_COUNT_THRESHOLD, speculativeMapReduceAbnormal.getFinishedMaps(),
                            BigDecimal.valueOf((speculativeMapReduceAbnormal.getFinishedMaps() - MAPREDUCE_MAP_TASK_COUNT_THRESHOLD))
                                    .multiply(BigDecimal.valueOf(MAPREDUCE_MAP_TASK_COUNT_SCORE)).doubleValue(),
                            MAPREDUCE_MAP_TASK_COUNT_DESC));
                } else {
                    res.put("MAPREDUCE_MAP_TASK_COUNT", new DiagnoseDesc("MAPREDUCE_MAP_TASK_COUNT", MAPREDUCE_MAP_TASK_COUNT_NAME,
                            MAPREDUCE_MAP_TASK_COUNT_THRESHOLD, speculativeMapReduceAbnormal.getFinishedMaps(),
                            0, MAPREDUCE_MAP_TASK_COUNT_DESC));
                }

                // reduce 数量过多
                if (speculativeMapReduceAbnormal.getFinishedReduces() > MAPREDUCE_REDUCE_TASK_COUNT_THRESHOLD) {
                    res.put("MAPREDUCE_REDUCE_TASK_COUNT", new DiagnoseDesc("MAPREDUCE_REDUCE_TASK_COUNT", MAPREDUCE_REDUCE_TASK_COUNT_NAME,
                            MAPREDUCE_REDUCE_TASK_COUNT_THRESHOLD, speculativeMapReduceAbnormal.getFinishedReduces(),
                            BigDecimal.valueOf((speculativeMapReduceAbnormal.getFinishedReduces() - MAPREDUCE_REDUCE_TASK_COUNT_THRESHOLD))
                                    .multiply(BigDecimal.valueOf(MAPREDUCE_REDUCE_TASK_COUNT_SCORE)).doubleValue(),
                            MAPREDUCE_REDUCE_TASK_COUNT_DESC));
                } else {
                    res.put("MAPREDUCE_REDUCE_TASK_COUNT", new DiagnoseDesc("MAPREDUCE_REDUCE_TASK_COUNT", MAPREDUCE_REDUCE_TASK_COUNT_NAME,
                            MAPREDUCE_REDUCE_TASK_COUNT_THRESHOLD, speculativeMapReduceAbnormal.getFinishedReduces(),
                            0, MAPREDUCE_REDUCE_TASK_COUNT_DESC));
                }
            }

            score = score.subtract(res.values().stream().map(x -> BigDecimal.valueOf(x.deductScore)).reduce((x, y) -> x.add(y)).orElse(BigDecimal.valueOf(0)));
        } catch (Exception e) {
            log.error("buildSqlScoreAbnormal fail. TaskName: " + taskApp.getTaskName() + "\tmsg: " + e.getMessage());
        }

        SqlScoreAbnormal diagnoseContent = new SqlScoreAbnormal();
        diagnoseContent.setDiagnoseResult(JSON.toJSONString(res));
        diagnoseContent.setScore(score.doubleValue());
        diagnoseContent.setAbnormal(score.doubleValue() < 60);
        return diagnoseContent;
    }

    @Data
    public static class DiagnoseDesc {
        private String diagnoseName;
        private String diagnoseDesc;
        private long threadThread;
        private long value;
        private double deductScore;
        private String desc;
        private Object remark;

        public DiagnoseDesc(String diagnoseName, String diagnoseDesc, long threadThread, long value, double deductScore, String desc) {
            this(diagnoseName, diagnoseDesc, threadThread, value, deductScore, desc, null);
        }

        public DiagnoseDesc(String diagnoseName, String diagnoseDesc, long threadThread, long value, double deductScore, String desc, Object remark) {
            this.diagnoseName = diagnoseName;
            this.diagnoseDesc = diagnoseDesc;
            this.threadThread = threadThread;
            this.value = value;
            this.deductScore = deductScore;
            this.desc = desc;
            this.remark = remark;
        }
    }
}


