package com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality;


import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.oppo.cloud.common.domain.elasticsearch.TaskApp;
import com.oppo.cloud.common.domain.eventlog.FileScanAbnormal;
import com.oppo.cloud.common.domain.eventlog.SqlScoreAbnormal;
import com.oppo.cloud.common.domain.eventlog.config.SqlScoreConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.Const.*;

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
    private static int getCommandLength(String command) {
        return command.replaceAll(" ", "").replaceAll("\n", "").length();
    }


    private static SqlScoreAbnormal buildSqlScoreAbnormal(DiagnoseResult diagnoseResult, SqlScoreConfig sqlScoreConfig) {
        SqlScoreAbnormal diagnoseContent = new SqlScoreAbnormal();
        StringBuffer sb = new StringBuffer();
        int deductScore = 0;

        if (diagnoseResult.getGroupByCount() > SQL_GROUP_BY_THRESHOLD) {
            int score = (diagnoseResult.getGroupByCount() - SQL_GROUP_BY_THRESHOLD) * SQL_GROUP_BY_SCORE;
            deductScore += score;
            sb.append("[SQL group by] 次数:" + diagnoseResult.getGroupByCount() + "，"
                    + "阈值:" + SQL_GROUP_BY_THRESHOLD + "，"
                    + "扣减分数:" + score + "。（"
                    + SQL_GROUP_BY_DESC + "）\n");
        }

        if (diagnoseResult.getUnionCount() > SQL_UNION_THRESHOLD) {
            int score = (diagnoseResult.getUnionCount() - SQL_UNION_THRESHOLD) * SQL_UNION_SCORE;
            deductScore += score;
            sb.append("[SQL UNION] 次数:" + diagnoseResult.getUnionCount() + "，"
                    + "阈值:" + SQL_UNION_THRESHOLD + "，"
                    + "扣减分数:" + score + "。（"
                    + SQL_UNION_DESC + "）\n");
        }

        if (diagnoseResult.getJoinCount() > SQL_JOIN_THRESHOLD) {
            int score = (diagnoseResult.getJoinCount() - SQL_JOIN_THRESHOLD) * SQL_JOIN_SCORE;
            deductScore += score;
            sb.append("[SQL join] 次数:" + diagnoseResult.getJoinCount() + "，"
                    + "阈值:" + SQL_JOIN_THRESHOLD + "，"
                    + "扣减分数:" + score + "。（"
                    + SQL_JOIN_DESC + "）\n");
        }

        if (diagnoseResult.getOrderByCount() > SQL_ORDER_BY_THRESHOLD) {
            int score = (diagnoseResult.getOrderByCount() - SQL_ORDER_BY_THRESHOLD) * SQL_ORDER_BY_SCORE;
            deductScore += score;
            sb.append("[SQL order by] 次数:" + diagnoseResult.getOrderByCount() + "，"
                    + "阈值:" + SQL_ORDER_BY_THRESHOLD + "，"
                    + "扣减分数:" + score + "。（"
                    + SQL_ORDER_BY_DESC + "）\n");
        }

        if (diagnoseResult.getSqlLength() > SQL_LENGTH_THRESHOLD) {
            int score = (((diagnoseResult.getSqlLength() - SQL_LENGTH_THRESHOLD) / 1000) + 1) * SQL_LENGTH_SCORE;
            deductScore += score;
            sb.append("[SQL长度] 长度:" + diagnoseResult.getSqlLength() + "，"
                    + "阈值:" + SQL_LENGTH_THRESHOLD + "，"
                    + "扣减分数:" + score + "。（"
                    + SQL_LENGTH_DESC + "）\n");
        }

        if (diagnoseResult.getRefTableMap().size() > SQL_READ_TABLE_THRESHOLD) {
            int score = (diagnoseResult.getRefTableMap().size() - SQL_READ_TABLE_THRESHOLD) * SQL_READ_TABLE_SCORE;
            deductScore += score;
            sb.append("[SQL读取表数量] 数量:" + diagnoseResult.getRefTableMap().size() + "，"
                    + "阈值:" + SQL_READ_TABLE_THRESHOLD + "，"
                    + "扣减分数:" + score + "。（"
                    + SQL_READ_TABLE_DESC + "）\n");
        }

        Map<String, Integer> refTableMap = diagnoseResult.getRefTableMap();
        for (String tableName : refTableMap.keySet()) {
            if (refTableMap.get(tableName) > SQL_TABLE_USE_THRESHOLD) {
                int score = (refTableMap.get(tableName) - SQL_TABLE_USE_THRESHOLD) * SQL_TABLE_USE_SCORE;
                deductScore += score;
                sb.append("[表使用次数] 表名:" + tableName
                        + "次数:" + refTableMap.get(tableName) + "，"
                        + "阈值:" + SQL_TABLE_USE_THRESHOLD + "，"
                        + "扣减分数:" + score + "。（"
                        + SQL_TABLE_USE_DESC + "）\n");
            }
        }

        // 扫描文件数量任务分布 : 20以内不扣分，-Ln(count-20)
        FileScanAbnormal.FileScanReport scriptReport = diagnoseResult.getScriptReport();
        if (scriptReport.getTotalFileCount() > SQL_SCAN_FILE_COUNT_THRESHOLD) {
            int score = (int) Math.ceil(Math.log(scriptReport.getTotalFileCount() - SQL_SCAN_FILE_COUNT_THRESHOLD));
            deductScore += score;
            sb.append("[SQL 扫描文件] 个数:" + scriptReport.getTotalFileCount() + "，"
                    + "阈值:" + SQL_SCAN_FILE_COUNT_THRESHOLD + "，"
                    + "扣减分数:" + score + "。（"
                    + SQL_SCAN_FILE_COUNT_DESC + "）\n");
        }

        // 扫描总文件大小任务分布：小于100M不扣分，-Ln(size-100M)
        if (scriptReport.getTotalFileSize() > SQL_SCAN_FILE_SIZE_THRESHOLD) {
            int score = (int) Math.ceil(Math.log((scriptReport.getTotalFileSize() - SQL_SCAN_FILE_SIZE_THRESHOLD) / 1024 * 1024.0));
            deductScore += score;
            sb.append("[SQL 扫描文件] 总大小:" + scriptReport.getTotalFileSize() + "Byte，"
                    + "阈值:" + SQL_SCAN_FILE_SIZE_THRESHOLD + "，"
                    + "扣减分数:" + score + "。（"
                    + SQL_SCAN_FILE_SIZE_DESC + "）\n");
        }

        // 扫描小文个数（小于10M）任务分布:10个以内不扣分，-根号(count-10)
        if (scriptReport.getLe10MFileCount() > SQL_SCAN_LE10M_FILE_COUNT_THRESHOLD) {
            int score = (int) Math.ceil(Math.sqrt(scriptReport.getLe10MFileCount() - SQL_SCAN_LE10M_FILE_COUNT_THRESHOLD));
            deductScore += score;
            sb.append("[SQL 扫描文件] 小文件个数:" + scriptReport.getLe10MFileCount() + "，"
                    + "阈值:" + SQL_SCAN_LE10M_FILE_COUNT_THRESHOLD + "，"
                    + "扣减分数:" + score + "。（"
                    + SQL_SCAN_LE10M_FILE_COUNT_DESC + "）\n");
        }

        // 扫描分区数量任务分布：1个以内不扣分，-count/10
        if (scriptReport.getPartitionCount() > SQL_SCAN_PARTITION_COUNT_THRESHOLD) {
            int score = (int) Math.ceil((scriptReport.getPartitionCount() - SQL_SCAN_PARTITION_COUNT_THRESHOLD) / 10.0);
            deductScore += score;
            sb.append("[SQL 扫描分区] 数量:" + scriptReport.getPartitionCount() + "，"
                    + "阈值:" + SQL_SCAN_PARTITION_COUNT_THRESHOLD + "，"
                    + "扣减分数:" + score + "。（"
                    + SQL_SCAN_PARTITION_COUNT_DESC + "）\n");
        }

        diagnoseContent.setDiagnoseResult(JSON.toJSONString(diagnoseResult));
        diagnoseContent.setScore(100 - deductScore);
        diagnoseContent.setAbnormal(diagnoseContent.getScore() < sqlScoreConfig.getMinScore());
        diagnoseContent.setScoreContent(sb.substring(0, sb.lastIndexOf("\n") > 0 ? sb.lastIndexOf("\n") : 0));
        return diagnoseContent;
    }


    /**
     * 获取表引用次数集合
     *
     * @param command    sql
     * @param scriptName 脚本名称
     * @return <tableName, refCount>
     */
    private static Map<String, Integer> getRefTableMap(String command, String scriptName) {
        Map<String, Integer> refTableMap = new HashMap<>();
        try {
            Map<String, Object> body = new HashMap<>();
            body.put("dbType", "Hive");
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
            log.error("getRefTableMap fail. scriptName：" + scriptName + ",msg：" + e.getMessage());
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
}
