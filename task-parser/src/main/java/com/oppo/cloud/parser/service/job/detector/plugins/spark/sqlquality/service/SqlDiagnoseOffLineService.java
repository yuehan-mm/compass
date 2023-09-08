package com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.service;


import com.alibaba.fastjson2.JSONObject;
import com.oppo.cloud.common.domain.eventlog.SqlScoreAbnormal;
import com.oppo.cloud.common.domain.eventlog.config.SqlScoreConfig;
import com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.bean.DiagnoseResult;
import com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.bean.ScriptInfo;
import com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.bean.SqlReport;
import com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.util.ExcelUtil;
import com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.util.MySqlUtils;
import com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.util.ReportBuilder;
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


    public static void buildReport(List<ScriptInfo> scriptInfos) {
        ReportBuilder reportBuilder = new ReportBuilder(scriptInfos);
        SqlReport sqlReport = reportBuilder.buildReport();
        System.out.println("REPORT:" + JSONObject.toJSONString(sqlReport));
    }

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
    public static List<ScriptInfo> getScriptList() {
        String sql = "select *  from t_script_sql_content where command!='' and script_type!='Sqoop'";
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

    public static void writeTable(List<ScriptInfo> scriptInfos) {
        System.out.println("ABNORMAL ROWS : " + scriptInfos.size());
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
                ps.setString(14, String.valueOf(System.currentTimeMillis()));
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

    public static void deleteData() {
        String sql = "delete from t_script_sql_diagnose_result ";
        System.out.println(sql);
        try {
            PreparedStatement prepareStatement = conn.prepareStatement(sql);
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
                        scriptInfo.getScript_name(), scriptInfo.getScript_type());
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
}
