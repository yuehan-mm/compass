package com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality;


import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.oppo.cloud.common.domain.eventlog.FileScanAbnormal;
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

    public static DiagnoseContent parseScript(String command, String taskName, FileScanAbnormal fileScanAbnormal) {
        return setDiagnoseInfo(findX(command, GROUP_BY_REGEX), findX(command, UNION_REGEX),
                findX(command, JOIN_REGEX), findX(command, ORDER_BY_REGEX),
                findY(command, INSERT_REGEX), findY(command, MEMORY_CONF_REGEX),
                fileScanAbnormal.getFileCount(), fileScanAbnormal.getAvgSize(),
                getCommandLength(command), getRefTableMap(command, taskName));
    }

    private static int getCommandLength(String command) {
        return command.replaceAll(" ", "").replaceAll("\n", "").length();
    }

    private static DiagnoseContent setDiagnoseInfo(int groupByCount, int unionCount, int joinCount, int orderByCount,
                                                   List<String> insertList, List<String> memConfList, int scanFileCount,
                                                   int scanFileSizeAvg, int sqlLength, Map<String, Integer> refTableMap) {
        DiagnoseContent diagnoseContent = new DiagnoseContent();
        StringBuffer sb = new StringBuffer();
        int deductScore = 0;

        if (groupByCount > SQL_GROUP_BY_THRESHOLD) {
            int score = (groupByCount - SQL_GROUP_BY_THRESHOLD) * SQL_GROUP_BY_SCORE;
            deductScore += score;
            sb.append("[SQL group by] 次数:" + groupByCount + "，"
                    + "阈值:" + SQL_GROUP_BY_THRESHOLD + "，"
                    + "扣减分数:" + score + "。（"
                    + SQL_GROUP_BY_DESC + "）\n");
        }

        if (unionCount > SQL_UNION_THRESHOLD) {
            int score = (unionCount - SQL_UNION_THRESHOLD) * SQL_UNION_SCORE;
            deductScore += score;
            sb.append("[SQL UNION] 次数:" + unionCount + "，"
                    + "阈值:" + SQL_UNION_THRESHOLD + "，"
                    + "扣减分数:" + score + "。（"
                    + SQL_UNION_DESC + "）\n");
        }

        if (joinCount > SQL_JOIN_THRESHOLD) {
            int score = (joinCount - SQL_JOIN_THRESHOLD) * SQL_JOIN_SCORE;
            deductScore += score;
            sb.append("[SQL join] 次数:" + joinCount + "，"
                    + "阈值:" + SQL_JOIN_THRESHOLD + "，"
                    + "扣减分数:" + score + "。（"
                    + SQL_JOIN_DESC + "）\n");
        }

        if (orderByCount > SQL_ORDER_BY_THRESHOLD) {
            int score = (orderByCount - SQL_ORDER_BY_THRESHOLD) * SQL_ORDER_BY_SCORE;
            deductScore += score;
            sb.append("[SQL order by] 次数:" + orderByCount + "，"
                    + "阈值:" + SQL_ORDER_BY_THRESHOLD + "，"
                    + "扣减分数:" + score + "。（"
                    + SQL_ORDER_BY_DESC + "）\n");
        }

        if (scanFileCount > SQL_SCAN_FILE_THRESHOLD) {
            int score = ((scanFileCount - SQL_SCAN_FILE_THRESHOLD) / 10 + 1) * SQL_SCAN_FILE_SCORE;
            deductScore += score;
            sb.append("[SQL 扫描文件] 个数:" + scanFileCount + "，"
                    + "阈值:" + SQL_SCAN_FILE_THRESHOLD + "，"
                    + "扣减分数:" + score + "。（"
                    + SQL_SCAN_FILE_DESC + "）\n");
        }

        if (scanFileSizeAvg < SQL_SCAN_FILE_SIZE_AVG_THRESHOLD) {
            deductScore += SQL_SCAN_FILE_SIZE_AVG_SCORE;
            sb.append("[SQL 扫描文件] 平均大小:" + scanFileSizeAvg + "Byte，"
                    + "阈值:" + SQL_SCAN_FILE_SIZE_AVG_THRESHOLD + "Byte，"
                    + "扣减分数:" + SQL_SCAN_FILE_SIZE_AVG_SCORE + "。（"
                    + SQL_SCAN_FILE_SIZE_AVG_DESC + "）\n");
        }

        if (sqlLength > SQL_LENGTH_THRESHOLD) {
            int score = (((sqlLength - SQL_LENGTH_THRESHOLD) / 1000) + 1) * SQL_LENGTH_SCORE;
            deductScore += score;
            sb.append("[SQL长度] 长度:" + sqlLength + "，"
                    + "阈值:" + SQL_LENGTH_THRESHOLD + "，"
                    + "扣减分数:" + score + "。（"
                    + SQL_LENGTH_DESC + "）\n");
        }

        if (refTableMap.size() > SQL_READ_TABLE_THRESHOLD) {
            int score = (refTableMap.size() - SQL_READ_TABLE_THRESHOLD) * SQL_READ_TABLE_SCORE;
            deductScore += score;
            sb.append("[SQL读取表数量] 数量:" + refTableMap.size() + "，"
                    + "阈值:" + SQL_READ_TABLE_THRESHOLD + "，"
                    + "扣减分数:" + score + "。（"
                    + SQL_READ_TABLE_DESC + "）\n");
        }

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

        diagnoseContent.setScore(100 - deductScore);
        diagnoseContent.setScoreContent(sb.substring(0, sb.lastIndexOf("\n") > 0 ? sb.lastIndexOf("\n") : 0));
        return diagnoseContent;
    }


    private static Map<String, Integer> getRefTableMap(String command, String scriptName) {
        Map<String, Integer> refTableMap = new HashMap<>();
        try {
            Map<String, Object> body = new HashMap<>();
            body.put("dbType", "Hive");
            body.put("originSQL", command);
            String jsonStr = HttpRequestUtils.doPost(REQUEST_URL, JSONObject.toJSONString(body));
            log.info("SQL Parse Result : " + jsonStr);
            JSONObject json = JSONObject.parseObject(jsonStr);
            if (json.getInteger("code") != 0) throw new RuntimeException(jsonStr);

            JSONArray dataArray = json.getJSONArray("data");
            for (int i = 0; i < dataArray.size(); i++) {
                JSONObject jsonObject = dataArray.getJSONObject(i);
                String tableName = jsonObject.getString("dbName") + "." + jsonObject.getString("tableName");
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
