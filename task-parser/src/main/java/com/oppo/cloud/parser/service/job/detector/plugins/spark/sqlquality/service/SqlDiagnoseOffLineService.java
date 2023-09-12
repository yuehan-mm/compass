package com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.service;


import com.oppo.cloud.common.domain.eventlog.SqlScoreAbnormal;
import com.oppo.cloud.common.domain.eventlog.config.SqlScoreConfig;
import com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.bean.DiagnoseResult;
import com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.bean.ScriptDiagnoseDetail;
import com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.bean.ScriptInfo;
import com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.util.ExcelUtil;
import com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.util.MySqlUtils;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.util.Const.*;

@Slf4j
public class SqlDiagnoseOffLineService {


    public static AtomicInteger failCount = new AtomicInteger();
    private static Connection conn = MySqlUtils.getConnection();

    public static void writeExcel(List<ScriptInfo> scriptInfos) {
        List<ScriptInfo> abNormalScriptList = scriptInfos.stream().filter(x -> x.getScore() < 60).collect(Collectors.toList());
        System.out.println("ABNORMAL ROWS : " + abNormalScriptList.size());
        abNormalScriptList.sort(Comparator.comparingDouble(o -> o.score));
        ExcelUtil.writeExcel(abNormalScriptList,
                "C:\\Users\\22047328\\Desktop\\work-"
                        + new SimpleDateFormat("yyyymmdd_HHmm").format(System.currentTimeMillis()) + ".xlsx");
    }

    /**
     * 获取单个表设计
     *
     * @return List<List < Object>>
     */
    public static List<ScriptDiagnoseDetail> getDiagnoseResultList() {
        String sql = "select *,\n" +
                "       ifnull(JSON_EXTRACT(JSON_EXTRACT(diagnose_result, '$.SQL_GROUP_BY'), '$.deductScore'),\n" +
                "              0)                                                                               SQL_GROUP_BY_DEDUCTSCORE,\n" +
                "       ifnull(JSON_EXTRACT(JSON_EXTRACT(diagnose_result, '$.SQL_GROUP_BY'), '$.value'), 0)     SQL_GROUP_BY_VALUE,\n" +
                "\n" +
                "       ifnull(JSON_EXTRACT(JSON_EXTRACT(diagnose_result, '$.SQL_UNION'), '$.deductScore'), 0)  SQL_UNION_DEDUCTSCORE,\n" +
                "       ifnull(JSON_EXTRACT(JSON_EXTRACT(diagnose_result, '$.SQL_UNION'), '$.value'), 0)        SQL_UNION_VALUE,\n" +
                "\n" +
                "       ifnull(JSON_EXTRACT(JSON_EXTRACT(diagnose_result, '$.SQL_JOIN'), '$.deductScore'), 0)   SQL_JOIN_DEDUCTSCORE,\n" +
                "       ifnull(JSON_EXTRACT(JSON_EXTRACT(diagnose_result, '$.SQL_JOIN'), '$.value'), 0)         SQL_JOIN_VALUE,\n" +
                "\n" +
                "       ifnull(JSON_EXTRACT(JSON_EXTRACT(diagnose_result, '$.SQL_ORDER_BY'), '$.deductScore'),\n" +
                "              0)                                                                               SQL_ORDER_BY_DEDUCTSCORE,\n" +
                "       ifnull(JSON_EXTRACT(JSON_EXTRACT(diagnose_result, '$.SQL_ORDER_BY'), '$.value'), 0)     SQL_ORDER_BY_VALUE,\n" +
                "\n" +
                "       ifnull(JSON_EXTRACT(JSON_EXTRACT(diagnose_result, '$.SQL_LENGTH'), '$.deductScore'), 0) SQL_LENGTH_DEDUCTSCORE,\n" +
                "       ifnull(JSON_EXTRACT(JSON_EXTRACT(diagnose_result, '$.SQL_LENGTH'), '$.value'), 0)       SQL_LENGTH_VALUE,\n" +
                "\n" +
                "       ifnull(JSON_EXTRACT(JSON_EXTRACT(diagnose_result, '$.SQL_TABLE_REF'), '$.deductScore'),\n" +
                "              0)                                                                               SQL_TABLE_REF_DEDUCTSCORE,\n" +
                "       ifnull(JSON_EXTRACT(JSON_EXTRACT(diagnose_result, '$.SQL_TABLE_REF'), '$.value'), 0)    SQL_TABLE_REF_VALUE,\n" +
                "\n" +
                "       ifnull(JSON_EXTRACT(JSON_EXTRACT(diagnose_result, '$.SQL_TABLE_READ'), '$.deductScore'),\n" +
                "              0)                                                                               SQL_TABLE_READ_DEDUCTSCORE,\n" +
                "       ifnull(JSON_EXTRACT(JSON_EXTRACT(diagnose_result, '$.SQL_TABLE_READ'), '$.value'), 0)   SQL_TABLE_READ_VALUE,\n" +
                "\n" +
                "       ifnull(JSON_EXTRACT(JSON_EXTRACT(diagnose_result, '$.SQL_SCAN_FILE_COUNT'), '$.deductScore'),\n" +
                "              0)                                                                               SQL_SCAN_FILE_COUNT_DEDUCTSCORE,\n" +
                "       ifnull(JSON_EXTRACT(JSON_EXTRACT(diagnose_result, '$.SQL_SCAN_FILE_COUNT'), '$.value'),\n" +
                "              0)                                                                               SQL_SCAN_FILE_COUNT_VALUE,\n" +
                "\n" +
                "       ifnull(JSON_EXTRACT(JSON_EXTRACT(diagnose_result, '$.SQL_SCAN_FILE_SIZE'), '$.deductScore'),\n" +
                "              0)                                                                               SQL_SCAN_FILE_SIZE_DEDUCTSCORE,\n" +
                "       ifnull(JSON_EXTRACT(JSON_EXTRACT(diagnose_result, '$.SQL_SCAN_FILE_SIZE'), '$.value'),\n" +
                "              0)                                                                               SQL_SCAN_FILE_SIZE_VALUE,\n" +
                "\n" +
                "       ifnull(JSON_EXTRACT(JSON_EXTRACT(diagnose_result, '$.SQL_SCAN_SMALL_FILE_COUNT'), '$.deductScore'),\n" +
                "              0)                                                                               SQL_SCAN_SMALL_FILE_COUNT_DEDUCTSCORE,\n" +
                "       ifnull(JSON_EXTRACT(JSON_EXTRACT(diagnose_result, '$.SQL_SCAN_SMALL_FILE_COUNT'), '$.value'),\n" +
                "              0)                                                                               SQL_SCAN_SMALL_FILE_COUNT_VALUE,\n" +
                "\n" +
                "       ifnull(JSON_EXTRACT(JSON_EXTRACT(diagnose_result, '$.SQL_SCAN_PARTITION_COUNT'), '$.deductScore'),\n" +
                "              0)                                                                               SQL_SCAN_PARTITION_COUNT_DEDUCTSCORE,\n" +
                "       ifnull(JSON_EXTRACT(JSON_EXTRACT(diagnose_result, '$.SQL_SCAN_PARTITION_COUNT'), '$.value'),\n" +
                "              0)                                                                               SQL_SCAN_PARTITION_COUNT_VALUE\n" +
                "\n" +
                "from t_script_sql_diagnose_result ";
        System.out.println(sql);
        List<ScriptDiagnoseDetail> fields = new ArrayList<>();
        try {
            Statement stat = conn.createStatement();
            ResultSet rs = stat.executeQuery(sql);
            while (rs.next()) {
                fields.add(new ScriptDiagnoseDetail(
                        rs.getString("script_id"), rs.getString("script_name"),
                        rs.getString("script_type"), rs.getString("command"),
                        rs.getString("db_type"), rs.getString("create_user_name"),
                        rs.getString("create_user_nickname"), rs.getString("group_id"),
                        rs.getString("group_name"), rs.getString("group_admin_name"),
                        rs.getString("group_admin_nickname"), rs.getString("root_group_id"),
                        rs.getString("root_group_name"), rs.getDouble("score"),
                        rs.getString("diagnose_result"), rs.getString("data_date"),

                        rs.getDouble("SQL_GROUP_BY_DEDUCTSCORE"), rs.getInt("SQL_GROUP_BY_VALUE"),
                        rs.getDouble("SQL_UNION_DEDUCTSCORE"), rs.getInt("SQL_UNION_VALUE"),
                        rs.getDouble("SQL_JOIN_DEDUCTSCORE"), rs.getInt("SQL_JOIN_VALUE"),
                        rs.getDouble("SQL_ORDER_BY_DEDUCTSCORE"), rs.getInt("SQL_ORDER_BY_VALUE"),
                        rs.getDouble("SQL_LENGTH_DEDUCTSCORE"), rs.getInt("SQL_LENGTH_VALUE"),
                        rs.getDouble("SQL_TABLE_REF_DEDUCTSCORE"), rs.getInt("SQL_TABLE_REF_VALUE"),
                        rs.getDouble("SQL_TABLE_READ_DEDUCTSCORE"), rs.getInt("SQL_TABLE_READ_VALUE"),
                        rs.getDouble("SQL_SCAN_FILE_COUNT_DEDUCTSCORE"), rs.getInt("SQL_SCAN_FILE_COUNT_VALUE"),
                        rs.getDouble("SQL_SCAN_FILE_SIZE_DEDUCTSCORE"), rs.getLong("SQL_SCAN_FILE_SIZE_VALUE"),
                        rs.getDouble("SQL_SCAN_SMALL_FILE_COUNT_DEDUCTSCORE"), rs.getInt("SQL_SCAN_SMALL_FILE_COUNT_VALUE"),
                        rs.getDouble("SQL_SCAN_PARTITION_COUNT_DEDUCTSCORE"), rs.getInt("SQL_SCAN_PARTITION_COUNT_VALUE")
                ));
            }
        } catch (SQLException e) {
            System.out.println("get script list fail." + e.getMessage());
            e.printStackTrace();
        }
        return fields;
    }

    /**
     * 获取单个表设计
     *
     * @return List<List < Object>>
     */
    public static List<ScriptInfo> getScriptList() {
        String sql = "select *  from t_script_sql_content where command!=''";
        System.out.println(sql);
        List<ScriptInfo> fields = new ArrayList<>();
        try {
            Statement stat = conn.createStatement();
            ResultSet rs = stat.executeQuery(sql);
            while (rs.next()) {
                fields.add(new ScriptInfo(
                        rs.getString("script_id"), rs.getString("script_name"),
                        rs.getString("script_type"), rs.getString("command"),
                        rs.getString("db_type"), rs.getString("create_user_name"),
                        rs.getString("create_user_nickname"), rs.getString("group_id"),
                        rs.getString("group_name"), rs.getString("group_admin_name"),
                        rs.getString("group_admin_nickname"), rs.getString("root_group_id"),
                        rs.getString("root_group_name")));
            }
        } catch (SQLException e) {
            System.out.println("get script list fail." + e.getMessage());
            e.printStackTrace();
        }
        return fields;
    }

    public static void writeTable(List<ScriptInfo> scriptInfos, String insertDataTime) {
        System.out.println("Start Write Data. Date:" + insertDataTime + " Count:" + scriptInfos.size());
        PreparedStatement ps = null;
        try {
            String sql = "INSERT INTO bdmp_cluster.t_script_sql_diagnose_result (script_id, script_name, script_type," +
                    " command, db_type, create_user_name, create_user_nickname, group_id, group_name," +
                    " group_admin_name, group_admin_nickname, diagnose_result, score, data_date," +
                    " root_group_id, root_group_name) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            ps = conn.prepareStatement(sql);
            for (int i = 0; i < scriptInfos.size(); i++) {
                ScriptInfo scriptInfo = scriptInfos.get(i);
                ps.setInt(1, Integer.parseInt(scriptInfo.getScript_id()));
                ps.setString(2, scriptInfo.getScript_name());
                ps.setString(3, scriptInfo.getScript_type());
                ps.setString(4, "");
                ps.setString(5, scriptInfo.getDb_type());
                ps.setString(6, scriptInfo.getCreate_user_id());
                ps.setString(7, scriptInfo.getCreate_user_name());
                ps.setInt(8, Integer.parseInt(scriptInfo.getCreate_user_group_id()));
                ps.setString(9, scriptInfo.getCreate_user_group_nane());
                ps.setString(10, scriptInfo.getCreate_user_group_admin_id());
                ps.setString(11, scriptInfo.getCreate_user_group_admin_name());
                ps.setString(12, scriptInfo.getDiagnoseResult());
                ps.setDouble(13, scriptInfo.getScore());
                ps.setString(14, insertDataTime);
                ps.setString(15, scriptInfo.getCreate_user_root_group_id());
                ps.setString(16, scriptInfo.getCreate_user_root_group_name());
                ps.addBatch();
                if (i % 1000 == 0) {
                    ps.executeBatch();
                    ps.clearBatch();
                }
            }
            ps.executeBatch();
        } catch (Exception e) {
            System.out.println("write table fail." + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                ps.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void deleteData(String removeDataTime) {
        String sql = "delete from t_script_sql_diagnose_result where data_date=? ";
        System.out.println("Delete Expire Data. sql: " + sql.replace("?", removeDataTime));
        try {
            PreparedStatement prepareStatement = conn.prepareStatement(sql);
            prepareStatement.setString(1, removeDataTime);
            prepareStatement.executeUpdate();
        } catch (SQLException e) {
            System.out.println("delete data fail." + e.getMessage());
            e.printStackTrace();
        }
    }


    public static void parseScript(List<ScriptInfo> scriptInfos) {
        SqlDiagnoseService sqlDiagnoseService = new SqlDiagnoseService();
        scriptInfos.stream().parallel().forEach(scriptInfo -> {
            String command = scriptInfo.getCommand().toLowerCase();

            Map<String, Integer> refTableMap;
            try {
                refTableMap = sqlDiagnoseService.getRefTableMap(command,
                        scriptInfo.getScript_name(), covertScriptType(scriptInfo.getScript_type()));
            } catch (Exception e) {
                failCount.incrementAndGet();
                refTableMap = new HashMap<>();
            }

            SqlScoreAbnormal sqlScoreAbnormal = sqlDiagnoseService.buildSqlScoreAbnormal(new DiagnoseResult(
                    null, null, sqlDiagnoseService.findX(command, GROUP_BY_REGEX),
                    sqlDiagnoseService.findX(command, UNION_REGEX), sqlDiagnoseService.findX(command, JOIN_REGEX),
                    sqlDiagnoseService.findX(command, ORDER_BY_REGEX), sqlDiagnoseService.getCommandLength(command),
                    refTableMap, null), new SqlScoreConfig());
            scriptInfo.setDiagnoseResult(sqlScoreAbnormal.getDiagnoseResult());
            scriptInfo.setScore(sqlScoreAbnormal.getScore());
        });
    }

    private static String covertScriptType(String scriptType) {
        return (scriptType.equals("Sqoop") || scriptType.equals("DBScript")) ? "mysql" : "hive";
    }
}
