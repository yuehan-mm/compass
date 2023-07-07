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

package com.oppo.cloud.application.service.impl;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.oppo.cloud.application.config.CustomConfig;
import com.oppo.cloud.application.config.HadoopConfig;
import com.oppo.cloud.application.config.KafkaConfig;
import com.oppo.cloud.application.constant.RetCode;
import com.oppo.cloud.application.constant.RetryException;
import com.oppo.cloud.application.domain.LogPathJoin;
import com.oppo.cloud.application.domain.ParseRet;
import com.oppo.cloud.application.domain.Rule;
import com.oppo.cloud.application.producer.MessageProducer;
import com.oppo.cloud.application.service.LogParserService;
import com.oppo.cloud.application.util.HDFSReaderCallback;
import com.oppo.cloud.application.util.HDFSUtil;
import com.oppo.cloud.application.util.StringUtil;
import com.oppo.cloud.common.constant.ApplicationType;
import com.oppo.cloud.common.domain.cluster.hadoop.NameNodeConf;
import com.oppo.cloud.mapper.TaskApplicationMapper;
import com.oppo.cloud.model.TaskApplication;
import com.oppo.cloud.model.TaskInstance;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 日志解析服务
 */
@Service
@Slf4j
public class LogParserServiceImpl implements LogParserService {

    /**
     * hadoop节点资源
     */
    @Autowired
    private HadoopConfig hadoopConfig;

    @Autowired
    private CustomConfig customConfig;

    @Autowired
    private KafkaConfig kafkaConfig;

    // SPRING_KAFKA_TASKINSTANCEAPPLICATION_TOPICS
    @Value("${spring.kafka.taskinstanceapplicationtopics}")
    private String TASKINSTANCEAPPLICATIONTOPICS;

    @Autowired
    private MessageProducer messageProducer;

    /**
     * 任务 application表管理
     */
    @Autowired
    private TaskApplicationMapper taskApplicationMapper;
    /**
     * hadoop文件读取节点配置
     */
    private Map<String, NameNodeConf> nameNodeMap;
    /**
     * 任务成功状态
     */
    private static final String TASK_STATE_SUCCESS = "success";

    /**
     * 任务失败状态
     */
    private static final String TASK_STATE_FAIL = "fail";

    /**
     * 任务其他状态
     */
    private static final String TASK_STATE_OTHER = "other";

    /**
     * Flink Task Type
     */
    private static final String TASK_TYPE_FLINK = "FLINK";

    /**
     * Spark Application Id
     */
    private static final String APPLICATION_ID = "applicationId";

    /**
     * 日志地址存储
     */
    private final static String LOG_PATH_KEY = "__log_path";

    @Value("${spring.hdfs.base-path}")
    private static String HDFS_BASE_PATH;

    /**
     * 获取hadoop集群配置信息
     */
    public synchronized Map<String, NameNodeConf> getNameNodeMap() {
        if (nameNodeMap == null) {
            nameNodeMap = initNameNode();
        }
        return nameNodeMap;
    }

    /**
     * 初始化配置信息
     */
    public Map<String, NameNodeConf> initNameNode() {
        Map<String, NameNodeConf> m = new HashMap<>();
        if (hadoopConfig.getNamenodes() == null) {
            return m;
        }

        for (NameNodeConf nameNodeConf : hadoopConfig.getNamenodes()) {
            m.put(nameNodeConf.getNameservices(), nameNodeConf);
        }
        return m;
    }

    /**
     * 任务处理
     */
    @Override
    public void handle(TaskInstance taskInstance) throws Exception {

        ApplicationMessage applicationMessage = new ApplicationMessage();


        LogParser logParser = new LogParser(taskInstance, customConfig.getRules(), applicationMessage);
        logParser.extract();

//        String logPath = String.join(",", applicationMessage.getLogPaths());
        // 保存 applicationId
        Map<String, ApplicationType> applications = applicationMessage.getApplications();

        for (Map.Entry<String, ApplicationType> application : applications.entrySet()) {
            addTaskApplication(application.getKey(), application.getValue(), taskInstance);
        }

        if (applications.size() > 0) {
            log.info("send to kafka. taskInstance: " + JSON.toJSONString(taskInstance));
            messageProducer.sendMessageSync(TASKINSTANCEAPPLICATIONTOPICS, JSON.toJSONString(taskInstance));
        } else {
            log.warn("can not match appid. taskInstance: " + JSON.toJSONString(taskInstance));
        }

        log.info("project: {}, process:{}, task:{}, execute_time: {}, parse applicationId done!",
                taskInstance.getProjectName(), taskInstance.getFlowName(), taskInstance.getTaskName(),
                taskInstance.getExecutionTime()
        );
    }

    /**
     * 添加任务applicationId
     */
    public void addTaskApplication(String applicationId, ApplicationType applicationType, TaskInstance taskInstance) {
        // 数据写回kafka订阅
        log.info("application save: applicationId=" + applicationId +
                " task_instance=" + taskInstance +
                ",appType=" + applicationType);

        TaskApplication taskApplication = new TaskApplication();
        taskApplication.setApplicationId(applicationId);
        taskApplication.setApplicationType(applicationType);
        taskApplication.setProjectName(taskInstance.getProjectName());
        taskApplication.setTaskName(taskInstance.getTaskName());
        taskApplication.setFlowName(taskInstance.getFlowName());
        taskApplication.setExecuteTime(taskInstance.getExecutionTime());
        taskApplication.setRetryTimes(taskInstance.getRetryTimes());
        taskApplication.setCreateTime(new Date());
        taskApplication.setUpdateTime(new Date());

        try {
            taskApplicationMapper.insertSelective(taskApplication);
        } catch (Exception e) {
            log.error("insertErr:" + e.getMessage() + "\t" + JSONObject.toJSONString(taskApplication));
        }

    }

    /**
     * 日志解析类
     */
    class LogParser {

        private TaskInstance taskInstance;
        private ApplicationMessage applicationMessage;

        private List<Rule> rules;

        private static final String TMP_EXTENSION = ".tmp";

        public LogParser(TaskInstance taskInstance, List<Rule> rules, ApplicationMessage applicationMessage) {
            this.taskInstance = taskInstance;
            this.rules = rules;
            this.applicationMessage = applicationMessage;
        }

        /**
         * 日志提取
         */
        public void extract() throws Exception {

            // 1. 拿到日志路径
            String logPath = this.getLogPath();

            if (StringUtils.isBlank(logPath)) {
                throw new IllegalArgumentException();
            }

            // 2. 拿到文件系统对应认证信息
            log.info("logPath:{}", logPath);
            NameNodeConf nameNodeConf = HDFSUtil.getNameNode(getNameNodeMap(), logPath);

            if (nameNodeConf == null) {
                throw new IllegalArgumentException("logPath: " + logPath + "{} does not have hadoop config");
            }

            // 3. 扫描文件
            List<String> filePaths;

            try {
                filePaths = HDFSUtil.filesPattern(nameNodeConf, String.format("%s*", logPath));
            } catch (Exception e) {
                throw new RetryException("filesPattern " + logPath + " error: ", e);
            }

            // 4. 判断有没有扫描到
            if (filePaths.size() == 0) {
                // 可能没来得及上传
                throw new RetryException("logPath: " + logPath + " does not exist，wait for retry");
            }

            // 5. 判断文件有没有处于正在写入状态的
            String tmpFile = getTmpFile(filePaths);
            if (tmpFile != null) {
                throw new RetryException("logPath is not reday: {}" + logPath);
            }

            /**
             * TODO
             *  fix：任务运行时间过长，调度日志过多，导致日志组合超长，无法插入task_application表
             *  fix：任务运行时间过长，调度日志过多，解析过多无用日志文件（心跳），影响 compass 性能
             *  bug：日志进行截取，取前20个日志文件，在有些极端情况下，前20个日志文件中不能完全匹配到 application_id
             *  bug: 日志进行截取，取前20个日志文件，task-parser分析其他异常时可能会错过一些信息
             */
            if (filePaths.size() > 20) {
                log.warn("Task: "
                        + taskInstance.getFlowName()
                        + "@"
                        + taskInstance.getTaskName()
                        + "@"
                        + taskInstance.getExecutionTime().toString()
                        + " has too many scheduler logs. count: "
                        + filePaths.size());
                filePaths = filePaths.subList(0, 20);
            }

            for (String filePath : filePaths) {
                try {
                    HDFSUtil.readLines(nameNodeConf, filePath, (String strLine) -> {
                        for (Rule rule : this.rules) {
                            Pattern pattern = rule.getPattern();
                            Matcher matcher = pattern.matcher(strLine);
                            if (matcher.matches()) {
                                if ((rule.getName() != null) && (rule.getName().trim().length() != 0)) {
                                    String appId = matcher.group(rule.getName());
                                    this.applicationMessage.addApplication(appId, rule.getType());
                                } else {
                                    String appId = String.format("v_%s_%s_%s_%s",
                                            taskInstance.getFlowName().length() > 20 ?
                                                    taskInstance.getFlowName().substring(0, 20) : taskInstance.getFlowName(),
                                            taskInstance.getTaskName().length() > 20 ?
                                                    taskInstance.getTaskName().substring(0, 20) : taskInstance.getTaskName(),
                                            taskInstance.getExecutionTime().getTime(),
                                            (int) (Math.random() * System.currentTimeMillis() % 1000)
                                    );
                                    this.applicationMessage.addApplication(appId, rule.getType());
                                }
                            }
                        }

                    });
                } catch (Exception e) {
                    throw new RetryException(e);
                }

            }
        }

        /**
         * 获取日志路径
         */
        public String getLogPath() {
            List<String> paths = new ArrayList<>();
            paths.add(HDFS_BASE_PATH);
            paths.add(this.taskInstance.getFlowName());
            paths.add(this.taskInstance.getTaskName());
            paths.add(convertTime(this.taskInstance.getExecutionTime())
                    .replace(":", "_")
                    .replace("+", "_")
                    + "-"
                    + this.taskInstance.getRetryTimes()
            );
            return String.join("/", paths);
        }

        private String convertTime(Date date) {
            String res = "";
            DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            DateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd'T'HH_mm_ssXXX");
            sdf2.setTimeZone(TimeZone.getTimeZone("UTC"));
            res = sdf2.format(date).replaceAll("Z", "_00_00");
            return res;
        }

        /**
         * If it contains temporary file, need to retry and wait for completion.
         */
        private String getTmpFile(List<String> filePaths) {
            for (String filePath : filePaths) {
                if (filePath.endsWith(TMP_EXTENSION)) {
                    return filePath;
                }
            }
            return null;
        }
    }

    static class ApplicationMessage {

        private Map<String, ApplicationType> applicationMap = new HashMap<>();

        public ApplicationMessage() {

        }

        public void addApplication(String appId, ApplicationType type) {
            this.applicationMap.put(appId, type);
        }

        public Map<String, ApplicationType> getApplications() {
            return applicationMap;
        }
    }
}
