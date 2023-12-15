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

import com.alibaba.fastjson2.JSONObject;
import com.oppo.cloud.common.domain.elasticsearch.TaskApp;
import com.oppo.cloud.common.domain.eventlog.MemWasteAbnormal;
import com.oppo.cloud.common.domain.eventlog.SqlScoreAbnormal;
import com.oppo.cloud.common.util.spring.SpringBeanUtil;
import com.oppo.cloud.parser.config.HdopDBConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.FastDateFormat;

import java.sql.*;

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
     * 保存SQL性能异常
     *
     * @param sqlScoreAbnormal
     * @param taskApp
     */
    public void saveJobPerformanceAbnormal(SqlScoreAbnormal sqlScoreAbnormal, TaskApp taskApp) {
        PreparedStatement ps = null;
        try {
            String sql = "INSERT INTO bdmp_cluster.t_job_performance_diagnose_result (application_id, application_type," +
                    " cluster_name, queue, task_name, start_time,end_time, elapsed_time, score, diagnose_result, data_date)" +
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, taskApp.getApplicationId());
            ps.setString(2, String.valueOf(taskApp.getApplicationType()));
            ps.setString(3, taskApp.getClusterName());
            ps.setString(4, taskApp.getQueue());
            ps.setString(5, taskApp.getTaskName());
            ps.setLong(6, taskApp.getStartTime().getTime());
            ps.setLong(7, taskApp.getFinishTime().getTime());
            ps.setDouble(8, taskApp.getElapsedTime());
            ps.setDouble(9, sqlScoreAbnormal.getScore());
            ps.setString(10, sqlScoreAbnormal.getDiagnoseResult());
            ps.setString(11, FastDateFormat.getInstance("yyyy-MM-dd").format(System.currentTimeMillis()));
            ps.execute();
        } catch (Exception e) {
            log.error("saveSqlScoreAbnormal fail. msg：{}", e.getMessage());
        } finally {
            try {
                if (ps != null) ps.close();
            } catch (SQLException e) {
                log.error("close PreparedStatement fail. msg:{}", e.getMessage());
            }
        }
    }

    public void saveJobPerformanceAbnormal2(SqlScoreAbnormal sqlScoreAbnormal, TaskApp taskApp) {
        PreparedStatement ps = null;
        try {
            String sql = "INSERT INTO bdmp_cluster.t_job_performance_diagnose_result (application_id, application_type," +
                    " cluster_name, queue, task_name, start_time,end_time, elapsed_time, score, diagnose_result, data_date)" +
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, taskApp.getApplicationId());
            ps.setString(2, String.valueOf(taskApp.getApplicationType()));
            ps.setString(3, taskApp.getClusterName());
            ps.setString(4, taskApp.getQueue());
            ps.setString(5, taskApp.getTaskName());
            ps.setLong(6, taskApp.getStartTime().getTime());
            ps.setLong(7, taskApp.getFinishTime().getTime());
            ps.setDouble(8, taskApp.getElapsedTime());
            ps.setDouble(9, sqlScoreAbnormal.getScore());
            ps.setString(10, sqlScoreAbnormal.getDiagnoseResult());
            ps.setString(11, "9999");
            ps.execute();
        } catch (Exception e) {
            log.error("saveSqlScoreAbnormal fail. msg：{}", e.getMessage());
        } finally {
            try {
                if (ps != null) ps.close();
            } catch (SQLException e) {
                log.error("close PreparedStatement fail. msg:{}", e.getMessage());
            }
        }
    }

    /**
     * 更新或保存任务性能评分（每个task每天可能会执行多次，只保留性能评分最低的一次）
     *
     * @param sqlScoreAbnormal
     * @param taskApp
     */
    public synchronized void saveOrUpdateJobPerformanceAbnormal(SqlScoreAbnormal sqlScoreAbnormal, TaskApp taskApp) {
        // TODO bug: will not return null
        Double currentScore = getJobPerformanceScore(taskApp);
        if (currentScore == null) {
            this.saveJobPerformanceAbnormal(sqlScoreAbnormal, taskApp);
        } else if (currentScore > sqlScoreAbnormal.getScore()) {
            this.deleteJobPerformanceAbnormal(taskApp);
            this.saveJobPerformanceAbnormal(sqlScoreAbnormal, taskApp);
        }
    }

    /**
     * 获取某个task今天最低的性能评分，如果今天还未执行过，则返回null
     *
     * @param taskApp
     * @return
     */
    public Double getJobPerformanceScore(TaskApp taskApp) {
        PreparedStatement ps = null;
        try {
            String sql = "SELECT MIN(score) score FROM bdmp_cluster.t_job_performance_diagnose_result WHERE task_name=? AND data_date=?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, taskApp.getTaskName());
            ps.setString(2, FastDateFormat.getInstance("yyyy-MM-dd").format(System.currentTimeMillis()));
            ResultSet resultSet = ps.executeQuery();
            if (resultSet.next()) {
                return resultSet.getDouble("score");
            }
        } catch (Exception e) {
            log.error("getJobPerformanceScore fail. msg：{}", e.getMessage());
        } finally {
            try {
                if (ps != null) ps.close();
            } catch (SQLException e) {
                log.error("close PreparedStatement fail. msg:{}", e.getMessage());
            }
        }
        return null;
    }

    public Double deleteJobPerformanceAbnormal(TaskApp taskApp) {
        PreparedStatement ps = null;
        try {
            String sql = "DELETE FROM bdmp_cluster.t_job_performance_diagnose_result WHERE task_name=? AND data_date=?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, StringUtils.isNotEmpty(taskApp.getTaskName()) ? taskApp.getTaskName() : "FF");
            ps.setString(2, FastDateFormat.getInstance("yyyy-MM-dd").format(System.currentTimeMillis()));
            ps.execute();
        } catch (Exception e) {
            log.error("saveSqlScoreAbnormal fail. msg：{}", e.getMessage());
        } finally {
            try {
                if (ps != null) ps.close();
            } catch (SQLException e) {
                log.error("close PreparedStatement fail. msg:{}", e.getMessage());
            }
        }
        return null;
    }

    public void saveJobMemWasteDAbnormal(MemWasteAbnormal memWasteAbnormal, TaskApp taskApp) {
        PreparedStatement ps = null;
        try {
            String sql = "INSERT INTO bdmp_cluster.t_job_mem_waste_diagnose_result (application_id, application_type," +
                    " cluster_name, queue, task_name, start_time,end_time, elapsed_time, driver_memory,executor_memory," +
                    " total_memory_time, total_memory_compute_time, waste_percent, data_date)" +
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, taskApp.getApplicationId());
            ps.setString(2, String.valueOf(taskApp.getApplicationType()));
            ps.setString(3, taskApp.getClusterName());
            ps.setString(4, taskApp.getQueue());
            ps.setString(5, taskApp.getTaskName());
            ps.setLong(6, taskApp.getStartTime().getTime());
            ps.setLong(7, taskApp.getFinishTime().getTime());
            ps.setDouble(8, taskApp.getElapsedTime());
            ps.setLong(9, memWasteAbnormal.getDriverMemory());
            ps.setLong(10, memWasteAbnormal.getExecutorMemory());
            ps.setLong(11, memWasteAbnormal.getTotalMemoryTime());
            ps.setLong(12, memWasteAbnormal.getTotalMemoryComputeTime());
            ps.setDouble(13, memWasteAbnormal.getWastePercent());
            ps.setString(14, FastDateFormat.getInstance("yyyy-MM-dd").format(System.currentTimeMillis()));
            ps.execute();
        } catch (Exception e) {
            log.error("saveJobMemWasteDAbnormal fail. msg：{}", e.getMessage());
        } finally {
            try {
                if (ps != null) ps.close();
            } catch (SQLException e) {
                log.error("close PreparedStatement fail. msg:{}", e.getMessage());
            }
        }
    }

    private void updateJobPerformanceAbnormal(SqlScoreAbnormal sqlScoreAbnormal, TaskApp taskApp) {
        PreparedStatement ps = null;
        try {
            String sql = "UPDATE bdmp_cluster.t_job_performance_diagnose_result SET application_id=?, application_type=?," +
                    " cluster_name=?, queue=?, start_time=?,end_time=?, elapsed_time=?, score=?, diagnose_result=?" +
                    " WHERE data_date=? and task_name=?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, taskApp.getApplicationId());
            ps.setString(2, String.valueOf(taskApp.getApplicationType()));
            ps.setString(3, taskApp.getClusterName());
            ps.setString(4, taskApp.getQueue());
            ps.setLong(5, taskApp.getStartTime().getTime());
            ps.setLong(6, taskApp.getFinishTime().getTime());
            ps.setDouble(7, taskApp.getElapsedTime());
            ps.setDouble(8, sqlScoreAbnormal.getScore());
            ps.setString(9, sqlScoreAbnormal.getDiagnoseResult());
            ps.setString(10, FastDateFormat.getInstance("yyyy-MM-dd").format(System.currentTimeMillis()));
            ps.setString(11, taskApp.getTaskName());
            int i = ps.executeUpdate();
            if (i != 1) {
                log.error("updateJobPerformanceAbnormal fail ：" + JSONObject.toJSONString(taskApp));
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

    /**
     * 更新或保存任务内存浪费（每个task每天可能会执行多次，只保留内存浪费最严重的一次）
     *
     * @param memWasteAbnormal
     * @param taskApp
     */
    public synchronized void saveOrUpdateJobMemWasteAbnormal(MemWasteAbnormal memWasteAbnormal, TaskApp taskApp) {
        // 获取浪费比例
        // TODO bug: will not return null
        Double wasteRate = getJobMemWasteRate(taskApp);
        if (wasteRate == null) {
            this.saveJobMemWasteDAbnormal(memWasteAbnormal, taskApp);
        } else if (memWasteAbnormal.getWastePercent() > wasteRate) {
            this.deleteJobMemWasteAbnormal(taskApp);
            this.saveJobMemWasteDAbnormal(memWasteAbnormal, taskApp);
        }
    }


    /**
     * 获取某个task今天最高的内存浪费率，如果今天还未执行过，则返回null
     *
     * @param taskApp
     * @return
     */
    public Double getJobMemWasteRate(TaskApp taskApp) {
        PreparedStatement ps = null;
        try {
            String sql = "SELECT MAX(waste_percent) percent FROM bdmp_cluster.t_job_mem_waste_diagnose_result " +
                    "WHERE task_name=? AND data_date=?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, taskApp.getTaskName());
            ps.setString(2, FastDateFormat.getInstance("yyyy-MM-dd").format(System.currentTimeMillis()));
            ResultSet resultSet = ps.executeQuery();
            if (resultSet.next()) {
                return resultSet.getDouble("percent");
            }
        } catch (Exception e) {
            log.error("getJobMemWasteRate fail. msg：{}", e.getMessage());
        } finally {
            try {
                if (ps != null) ps.close();
            } catch (SQLException e) {
                log.error("close PreparedStatement fail. msg:{}", e.getMessage());
            }
        }
        return null;
    }

    public Double deleteJobMemWasteAbnormal(TaskApp taskApp) {
        PreparedStatement ps = null;
        try {
            String sql = "DELETE FROM bdmp_cluster.t_job_mem_waste_diagnose_result WHERE task_name=? AND data_date=?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, StringUtils.isNotEmpty(taskApp.getTaskName()) ? taskApp.getTaskName() : "FF");
            ps.setString(2, FastDateFormat.getInstance("yyyy-MM-dd").format(System.currentTimeMillis()));
            ps.execute();
        } catch (Exception e) {
            log.error("saveSqlScoreAbnormal fail. msg：{}", e.getMessage());
        } finally {
            try {
                if (ps != null) ps.close();
            } catch (SQLException e) {
                log.error("close PreparedStatement fail. msg:{}", e.getMessage());
            }
        }
        return null;
    }


    private void updateJobMemWasteAbnormal(MemWasteAbnormal memWasteAbnormal, TaskApp taskApp) {
        PreparedStatement ps = null;
        try {
            String sql = "UPDATE bdmp_cluster.t_job_mem_waste_diagnose_result set application_id=?, application_type=?," +
                    " cluster_name=?, queue=?, start_time=?,end_time=?, elapsed_time=?, driver_memory=?,executor_memory=?," +
                    " total_memory_time=?, total_memory_compute_time=?, waste_percent=? where data_date=? and  task_name=? ";
            ps = connection.prepareStatement(sql);
            ps.setString(1, taskApp.getApplicationId());
            ps.setString(2, String.valueOf(taskApp.getApplicationType()));
            ps.setString(3, taskApp.getClusterName());
            ps.setString(4, taskApp.getQueue());
            ps.setLong(5, taskApp.getStartTime().getTime());
            ps.setLong(6, taskApp.getFinishTime().getTime());
            ps.setDouble(7, taskApp.getElapsedTime());
            ps.setLong(8, memWasteAbnormal.getDriverMemory());
            ps.setLong(9, memWasteAbnormal.getExecutorMemory());
            ps.setLong(10, memWasteAbnormal.getTotalMemoryTime());
            ps.setLong(11, memWasteAbnormal.getTotalMemoryComputeTime());
            ps.setDouble(12, memWasteAbnormal.getWastePercent());
            ps.setString(13, FastDateFormat.getInstance("yyyy-MM-dd").format(System.currentTimeMillis()));
            ps.setString(14, taskApp.getTaskName());
            ps.execute();
        } catch (Exception e) {
            log.error("saveJobMemWasteDAbnormal fail. msg：{}", e.getMessage());
        } finally {
            try {
                if (ps != null) ps.close();
            } catch (SQLException e) {
                log.error("close PreparedStatement fail. msg:{}", e.getMessage());
            }
        }
    }

}