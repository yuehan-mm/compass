/*
 * Copyright 2023 OPPO.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.oppo.cloud.parser.service.writer;

import com.alibaba.fastjson2.JSON;
import com.oppo.cloud.common.domain.eventlog.SqlScoreAbnormal;
import com.oppo.cloud.common.util.spring.SpringBeanUtil;
import com.oppo.cloud.parser.config.HdopDBConfig;
import com.oppo.cloud.parser.domain.job.TaskParam;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * MysqlWriter
 */
@Slf4j
public class MysqlWriter {
    private static MysqlWriter mysqlWriter;

    public Connection connection;

    private MysqlWriter() {
        HdopDBConfig yml = (HdopDBConfig) SpringBeanUtil.getBean(HdopDBConfig.class);
        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection(yml.getUrl(), yml.getUsername(), yml.getPassword());
            log.info("get mysql connection success. url:{} .", yml.getUrl());
        } catch (Exception e) {
            log.error("get mysql connection fail. msg: {}, url:{} .", e.getMessage(), yml.getUrl());
        }
    }

    public synchronized static MysqlWriter getInstance() {
        if (mysqlWriter == null) {
            mysqlWriter = new MysqlWriter();
        }
        return mysqlWriter;
    }


    /**
     * 更新离线数据，工单系统目前仍然使用的离线数据
     *
     * @param sqlScoreAbnormal
     * @param taskParam
     */
    public void updateOffLineData(SqlScoreAbnormal sqlScoreAbnormal, TaskParam taskParam) {
        PreparedStatement ps = null;
        try {
            String sql = "UPDATE bdmp_cluster.t_script_sql_diagnose_result SET score=?,score_content=?,diagnose_result=?,data_date=? where script_name =?";
            ps = connection.prepareStatement(sql);
            ps.setInt(1, sqlScoreAbnormal.getScore());
            ps.setString(2, sqlScoreAbnormal.getScoreContent());
            ps.setString(3, sqlScoreAbnormal.getDiagnoseResult());
            ps.setString(4, String.valueOf(System.currentTimeMillis()));
            ps.setString(5, taskParam.getTaskApp().getTaskName());
            int effectiveRow = ps.executeUpdate();
            if (effectiveRow != 1) {
                log.error("update updateOffLineData fail. effectiveRow: {} , script_name:{}",
                        effectiveRow, taskParam.getTaskApp().getTaskName());
            }
        } catch (Exception e) {
            log.error("updateOffLineData fail. msg：{}", e.getMessage());
        } finally {
            try {
                if (ps != null) ps.close();
            } catch (SQLException e) {
                log.error("close PreparedStatement fail. msg:{}", e.getMessage());
            }
        }
    }

    public void updateOffLineData2(String scanFileReport, TaskParam taskParam) {
        PreparedStatement ps = null;
        try {
            String sql = "UPDATE bdmp_cluster.t_script_sql_diagnose_result SET scan_file_report=? where script_name =?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, scanFileReport);
            ps.setString(2, taskParam.getTaskApp().getTaskName());
            int effectiveRow = ps.executeUpdate();
            if (effectiveRow != 1) {
                log.error("update updateOffLineData fail. effectiveRow: {} , script_name:{}",
                        effectiveRow, taskParam.getTaskApp().getTaskName());
            }
        } catch (Exception e) {
            log.error("updateOffLineData fail. msg：{}", e.getMessage());
        } finally {
            try {
                if (ps != null) ps.close();
            } catch (SQLException e) {
                log.error("close PreparedStatement fail. msg:{}", e.getMessage());
            }
        }
    }
}
