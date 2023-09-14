package com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.service;


import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.oppo.cloud.common.domain.elasticsearch.TaskApp;
import com.oppo.cloud.common.domain.eventlog.FileScanAbnormal;
import com.oppo.cloud.common.domain.eventlog.SqlScoreAbnormal;
import com.oppo.cloud.common.domain.eventlog.config.SqlScoreConfig;
import com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.bean.DiagnoseResult;
import com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.util.HttpRequestUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.util.Const.*;

@Slf4j
public class SqlDiagnoseService {

    /**
     * 构建SQL评分异常
     *
     * @param command          SQL
     * @param taskApp          APP 运行信息
     * @param fileScanAbnormal SQL文件扫描信息
     * @param sqlScoreConfig   sql评分配置
     * @return
     */
    public static SqlScoreAbnormal buildSqlScoreAbnormal(String command, TaskApp taskApp, FileScanAbnormal fileScanAbnormal, SqlScoreConfig sqlScoreConfig) {
        return buildSqlScoreAbnormal(new DiagnoseResult(
                taskApp.getExecutionDate(), taskApp.getApplicationId(),
                findX(command, GROUP_BY_REGEX), findX(command, UNION_REGEX),
                findX(command, JOIN_REGEX), findX(command, ORDER_BY_REGEX),
                getCommandLength(command), getRefTableMap(command, taskApp.getTaskName()),
                fileScanAbnormal.getScriptReport()), sqlScoreConfig);
    }

    /**
     * 获取SQL长度，去除换行和空格
     *
     * @param command
     * @return
     */
    public static int getCommandLength(String command) {
        return command.replaceAll(" ", "").replaceAll("\n", "").length();
    }


    public static SqlScoreAbnormal buildSqlScoreAbnormal(DiagnoseResult diagnoseResult, SqlScoreConfig sqlScoreConfig) {
        LinkedHashMap<String, DiagnoseDesc> res = new LinkedHashMap<>();

        if (diagnoseResult.getSqlLength() > SQL_LENGTH_THRESHOLD) {
            res.put("SQL_LENGTH", new DiagnoseDesc("SQL_LENGTH", SQL_LENGTH_NAME,
                    SQL_LENGTH_THRESHOLD, diagnoseResult.getSqlLength(),
                    BigDecimal.valueOf((((diagnoseResult.getSqlLength() - SQL_LENGTH_THRESHOLD) / 1000) + 1))
                            .multiply(BigDecimal.valueOf(SQL_LENGTH_SCORE)).doubleValue(),
                    SQL_LENGTH_DESC));
        } else {
            res.put("SQL_LENGTH", new DiagnoseDesc("SQL_LENGTH", SQL_LENGTH_NAME,
                    SQL_LENGTH_THRESHOLD, diagnoseResult.getSqlLength(),
                    0, SQL_LENGTH_DESC));
        }

        if (diagnoseResult.getRefTableMap().size() > SQL_TABLE_ERF_THRESHOLD) {
            res.put("SQL_TABLE_REF", new DiagnoseDesc("SQL_TABLE_REF", SQL_TABLE_ERF_NAME,
                    SQL_TABLE_ERF_THRESHOLD, diagnoseResult.getRefTableMap().size(),
                    BigDecimal.valueOf((diagnoseResult.getRefTableMap().size() - SQL_TABLE_ERF_THRESHOLD))
                            .multiply(BigDecimal.valueOf(SQL_TABLE_ERF_SCORE)).doubleValue(),
                    SQL_TABLE_ERF_DESC, diagnoseResult.getRefTableMap().keySet()));
        } else {
            res.put("SQL_TABLE_REF", new DiagnoseDesc("SQL_TABLE_REF", SQL_TABLE_ERF_NAME,
                    SQL_TABLE_ERF_THRESHOLD, diagnoseResult.getRefTableMap().size(), 0,
                    SQL_TABLE_ERF_DESC, diagnoseResult.getRefTableMap().keySet()));
        }

        Integer readTableCount = diagnoseResult.getRefTableMap().values().stream().reduce((x, y) -> x + y).orElse(0);
        if (readTableCount > SQL_TABLE_READ_THRESHOLD) {
            res.put("SQL_TABLE_READ", new DiagnoseDesc("SQL_TABLE_READ", SQL_TABLE_READ_NAME,
                    SQL_TABLE_READ_THRESHOLD, readTableCount,
                    BigDecimal.valueOf((readTableCount - SQL_TABLE_READ_THRESHOLD))
                            .multiply(BigDecimal.valueOf(SQL_TABLE_READ_SCORE)).doubleValue(),
                    SQL_TABLE_READ_DESC, diagnoseResult.getRefTableMap()));
        } else {
            res.put("SQL_TABLE_READ", new DiagnoseDesc("SQL_TABLE_READ", SQL_TABLE_READ_NAME,
                    SQL_TABLE_READ_THRESHOLD, readTableCount,
                    0, SQL_TABLE_READ_DESC, diagnoseResult.getRefTableMap()));
        }

        if (diagnoseResult.getUnionCount() > SQL_UNION_THRESHOLD) {
            res.put("SQL_UNION", new DiagnoseDesc("SQL_UNION", SQL_UNION_NAME,
                    SQL_UNION_THRESHOLD, diagnoseResult.getUnionCount(),
                    BigDecimal.valueOf((diagnoseResult.getUnionCount() - SQL_UNION_THRESHOLD))
                            .multiply(BigDecimal.valueOf(SQL_UNION_SCORE)).doubleValue(),
                    SQL_UNION_DESC));
        } else {
            res.put("SQL_UNION", new DiagnoseDesc("SQL_UNION", SQL_UNION_NAME,
                    SQL_UNION_THRESHOLD, diagnoseResult.getUnionCount(),
                    0, SQL_UNION_DESC));
        }

        if (diagnoseResult.getJoinCount() > SQL_JOIN_THRESHOLD) {
            res.put("SQL_JOIN", new DiagnoseDesc("SQL_JOIN", SQL_JOIN_NAME,
                    SQL_JOIN_THRESHOLD, diagnoseResult.getJoinCount(),
                    BigDecimal.valueOf((diagnoseResult.getJoinCount() - SQL_JOIN_THRESHOLD))
                            .multiply(BigDecimal.valueOf(SQL_JOIN_SCORE)).doubleValue(),
                    SQL_JOIN_DESC));
        } else {
            res.put("SQL_JOIN", new DiagnoseDesc("SQL_JOIN", SQL_JOIN_NAME,
                    SQL_JOIN_THRESHOLD, diagnoseResult.getJoinCount(),
                    0, SQL_JOIN_DESC));
        }

        if (diagnoseResult.getGroupByCount() > SQL_GROUP_BY_THRESHOLD) {
            res.put("SQL_GROUP_BY", new DiagnoseDesc("SQL_GROUP_BY", SQL_GROUP_BY_NAME,
                    SQL_GROUP_BY_THRESHOLD, diagnoseResult.getGroupByCount(),
                    BigDecimal.valueOf((diagnoseResult.getGroupByCount() - SQL_GROUP_BY_THRESHOLD))
                            .multiply(BigDecimal.valueOf(SQL_GROUP_BY_SCORE)).doubleValue(),
                    SQL_GROUP_BY_DESC));
        } else {
            res.put("SQL_GROUP_BY", new DiagnoseDesc("SQL_GROUP_BY", SQL_GROUP_BY_NAME,
                    SQL_GROUP_BY_THRESHOLD, diagnoseResult.getGroupByCount(),
                    0, SQL_GROUP_BY_DESC));
        }


        if (diagnoseResult.getOrderByCount() > SQL_ORDER_BY_THRESHOLD) {
            res.put("SQL_ORDER_BY", new DiagnoseDesc("SQL_ORDER_BY", SQL_ORDER_BY_NAME,
                    SQL_ORDER_BY_THRESHOLD, diagnoseResult.getOrderByCount(),
                    BigDecimal.valueOf((diagnoseResult.getOrderByCount() - SQL_ORDER_BY_THRESHOLD))
                            .multiply(BigDecimal.valueOf(SQL_ORDER_BY_SCORE)).doubleValue(),
                    SQL_ORDER_BY_DESC));
        } else {
            res.put("SQL_ORDER_BY", new DiagnoseDesc("SQL_ORDER_BY", SQL_ORDER_BY_NAME,
                    SQL_ORDER_BY_THRESHOLD, diagnoseResult.getOrderByCount(),
                    0,
                    SQL_ORDER_BY_DESC));
        }


        //-------文件类-------//
        // 扫描文件数量
        FileScanAbnormal.FileScanReport fileScanReport = diagnoseResult.getFileScanReport();
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

        SqlScoreAbnormal diagnoseContent = new SqlScoreAbnormal();
        diagnoseContent.setDiagnoseResult(JSON.toJSONString(res));
        diagnoseContent.setScore(100.0 - res.values().stream().map(x -> x.getDeductScore()).reduce((x, y) -> x + y).orElse(0.0));
        diagnoseContent.setAbnormal(diagnoseContent.getScore() < sqlScoreConfig.getMinScore());
        return diagnoseContent;
    }


    /**
     * 调用此方法的必然是hive或者spark脚本
     *
     * @param command    sql
     * @param scriptName 脚本名称
     * @return <tableName, refCount>
     */
    private static Map<String, Integer> getRefTableMap(String command, String scriptName) {
        Map<String, Integer> refTableMap;
        try {
            refTableMap = getRefTableMap(command, scriptName, "hive");
        } catch (Exception e) {
            refTableMap = new HashMap<>();
        }
        return refTableMap;
    }

    /**
     * @param command    sql
     * @param scriptName 脚本名称
     * @param scriptType 脚本类型
     * @return <tableName, refCount>
     */
    public static Map<String, Integer> getRefTableMap(String command, String scriptName, String scriptType) throws RuntimeException {
        long ts = System.currentTimeMillis();
        Map<String, Integer> refTableMap = new HashMap<>();
        try {
            Map<String, Object> body = new HashMap<>();
            body.put("dbType", scriptType);
            body.put("originSQL", command);
            String jsonStr = HttpRequestUtils.doPost(REQUEST_URL, JSONObject.toJSONString(body));
            JSONObject json = JSONObject.parseObject(jsonStr);
            if (json.getInteger("code") != 0) throw new RuntimeException(jsonStr);

            JSONArray dataArray = json.getJSONArray("data");
            for (int i = 0; i < dataArray.size(); i++) {
                JSONObject jsonObject = dataArray.getJSONObject(i);
                String tableName = jsonObject.getString("tableName").toLowerCase();
                if (!refTableMap.containsKey(tableName)) {
                    refTableMap.put(tableName, findX(command, TABLE_NAME_REGEX.replace("TABLE_NAME", tableName)));
                }
            }
        } catch (Exception e) {
            log.error("getRefTableMap fail. scriptName:{},msg:{},timeUsed:{}", scriptName, e.getMessage(), System.currentTimeMillis() - ts);
            throw new RuntimeException(e);
        }
        return refTableMap;
    }


    public static int findX(String str, String regex) {
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(str);
        if (matcher.find()) {
            return findX(str.substring(matcher.end()), regex) + 1;
        } else {
            return 0;
        }
    }

    public static List<String> findY(String str, String regex) {
        List<String> res = new ArrayList<>();
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(str);
        if (matcher.find()) {
            res.add(str.substring(matcher.start(), matcher.end()).trim());
            res.addAll(findY(str.substring(matcher.end()), regex));
        }
        return res;
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


