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

    @Autowired
    private MessageProducer messageProducer;

    /**
     * 原生sql查询
     */
    @Autowired
    private JdbcTemplate jdbcTemplate;

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
    private String HDFS_BASE_PATH;

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
     * filter task instance without matching rule.
//     */
//    public boolean skipTaskInstance(TaskInstance taskInstance) {
//        // 实例状态为空或者非成功、失败状态
//        if (StringUtils.isBlank(taskInstance.getTaskState())) {
//            return true;
//        }
//        if (taskInstance.getTaskState().equals(TASK_STATE_OTHER)) {
//            if (StringUtils.isBlank(taskInstance.getTaskType())
//                    || !taskInstance.getTaskType().equals(TASK_TYPE_FLINK)) {
//                return true;
//            }
//        }
//        return false;
//    }

    /**
     * 任务处理
     */
    @Override
    public void handle(TaskInstance taskInstance, Map<String, String> rawData) throws Exception {
//        if (skipTaskInstance(taskInstance)) {
//            return new ParseRet(RetCode.RET_SKIP, null);
//        }
        // 获取完整的数据
//        Map<String, Object> data;
//        String sql = null;
//        Object[] args = null;
//        try {
//            if (taskInstance.getId() != null && taskInstance.getId() != 0) {
//                sql = "SELECT * FROM task_instance WHERE id = ?";
//                args = new Object[]{taskInstance.getId()};
//            } else {
//                sql = "SELECT * FROM task_instance WHERE flow_name = ? and task_name = ? and execution_time = ?";
//                args = new Object[]{taskInstance.getFlowName(), taskInstance.getTaskName(),
//                        taskInstance.getExecutionTime()};
//            }
//            data = jdbcTemplate.queryForMap(sql, args);
//        } catch (Exception e) {
//            log.error("exception:" + e + ", sql=" + sql + ", args=" + Arrays.toString(args) + ",taskInstance="
//                    + taskInstance);
//            return new ParseRet(RetCode.RET_EXCEPTION, taskInstance);
//        }
//        // 补充其他数据依赖
//        if (rawData != null) {
//            for (Map.Entry<String, String> map : rawData.entrySet()) {
//                if (!data.containsKey(map.getKey())) {
//                    data.put(map.getKey(), map.getValue());
//                }
//            }
//        }
//        // 获取task instance全部信息
//        taskInstance = new JSONObject(data).toJavaObject(TaskInstance.class);
//
//        int count = 0;
//        List<String> logPathList = new ArrayList<>();
//        ParseRet parseRet = new ParseRet(RetCode.RET_SKIP, null);

        ApplicationMessage applicationMessage = new ApplicationMessage();

        for (Rule rule : customConfig.getRules()) {
            LogParser logParser = new LogParser(taskInstance, rule, applicationMessage);
            logParser.extract();
        }


//            try {
//                RetCode retCode = logParser.extract();
//                parseRet = new ParseRet(retCode, taskInstance);
//                data = logParser.getData();
//                logPathList.add((String) data.get(LOG_PATH_KEY));
//                if (retCode != RetCode.RET_OK) {
//                    break;
//                }
//                count++; // 计数
//            } catch (Exception e) {
//                log.error("parseError:", e);
//                parseRet = new ParseRet(RetCode.RET_EXCEPTION, taskInstance);
//                break;
//            }
//        }

//        log.debug("parseRet==>" + parseRet);
//        // 非成功解析的返回
//        if (parseRet.getRetCode() != RetCode.RET_OK && parseRet.getRetCode() != RetCode.RET_DATA_NOT_EXIST) {
//            return parseRet;
//        }

        String logPath = String.join(",", applicationMessage.getLogPaths());
        // 保存 applicationId
        Set<String> applicationIds = applicationMessage.getApplicationIds();

        for (String appId :  applicationIds) {
            addTaskApplication((String) appId, taskInstance, logPath);
        }

        log.info("project: {}, process:{}, task:{}, execute_time: {}, parse applicationId done!",
                taskInstance.getProjectName(), taskInstance.getFlowName(), taskInstance.getTaskName(),
                taskInstance.getExecutionTime());
    }

    /**
     * 添加任务applicationId
     */
    public void addTaskApplication(String applicationId, TaskInstance taskInstance, String logPath) {
        // 数据写回kafka订阅
        log.info("application save: applicationId=" + applicationId + " task_instance=" + taskInstance + ",lopPath="
                + logPath);

        TaskApplication taskApplication = new TaskApplication();
        taskApplication.setApplicationId(applicationId);
        taskApplication.setProjectName(taskInstance.getProjectName());
        taskApplication.setTaskName(taskInstance.getTaskName());
        taskApplication.setFlowName(taskInstance.getFlowName());
        taskApplication.setExecuteTime(taskInstance.getExecutionTime());
        taskApplication.setRetryTimes(taskInstance.getRetryTimes());
        taskApplication.setLogPath(logPath);
        taskApplication.setCreateTime(new Date());
        taskApplication.setUpdateTime(new Date());

        try {
            taskApplicationMapper.insertSelective(taskApplication);
        } catch (DuplicateKeyException e) {
            return;
            // duplicate key with return
        } catch (Exception e) {
            log.error("insertErr:" + e.getMessage());
        }

    }

    /**
     * 日志解析类
     */
    class LogParser {

        /**
         * 中间数据存储
         */
        private TaskInstance taskInstance;
        private ApplicationMessage applicationMessage;

        /**
         * 日志解析规则
         */
        private Rule rule;

//        /**
//         * 解析次序
//         */
//        private final int index;


        /**
         * 读取hadoop文件延迟时间
         */
        private final int[] SLEEP_TIME = new int[]{20, 40, 60, 80, 100};

        private static final String TMP_EXTENSION = ".tmp";

        /**
         * task type
         */
        private String taskType = "";

        public LogParser(TaskInstance taskInstance, Rule rule, ApplicationMessage applicationMessage) {
            this.taskInstance = taskInstance;
            this.rule = rule;
            this.applicationMessage =applicationMessage;
//            this.data = data;
//            this.rule = rule;
//            this.index = index;
//
//            if (taskType != null) {
//                this.taskType = taskType;
//            }

        }

        /**
         * 日志提取
         */
        public void extract() throws Exception {
//            if (!StringUtils.isBlank(rule.getLogPathDep().getQuery())) {
//                String sql = StringUtil.replaceParams(rule.getLogPathDep().getQuery(), data);
//                log.info("extract SQL:{}, data:{}", sql, data);
//                Map<String, Object> depData = null;
//                try {
//                    depData = jdbcTemplate.queryForMap(sql);
//                } catch (Exception e) {
//                    log.error(e.getMessage());
//                    return RetCode.RET_EXCEPTION;
//                }
//
//                for (String key : depData.keySet()) {
//                    data.put(key, depData.get(key));
//                }
//            }

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

            filePaths.stream().forEach(path -> applicationMessage.addLogPath(path));

            // 记录日志路径
//            logPath = String.join(",", filePaths);
//            data.put(LOG_PATH_KEY, logPath);


//            int countFileIfHasContent = 0;
            Pattern pattern = Pattern.compile(rule.getExtractLog().getRegex());

            for (String filePath : filePaths) {
                HDFSUtil.readLines(nameNodeConf, filePath, new HDFSReaderCallback() {
                    @Override
                    public void call(String strLine) {
                        Matcher matcher = pattern.matcher(strLine);
                        if (matcher.matches()) {
                            String appId = matcher.group(rule.getExtractLog().getName());
                            LogParser.this.applicationMessage.addApplicationId(appId);
                        }
                    }
                });
            }
        }
//
//                if (lines.length > 0) {
//                    countFileIfHasContent += 1;
//                }
//                // 提取关键字
//                for (String line : lines) {
//                    Matcher matcher = pattern.matcher(line);
//                    if (matcher.matches()) {
//                        String matchVal = matcher.group(rule.getExtractLog().getName());
//                        applicationIds.add(matchVal);
////
////                        if (this.data.get(rule.getExtractLog().getName()) != null) { // 值已经存在， 原来值变为列表类型
////                            Object val = this.data.get(rule.getExtractLog().getName());
////                            // 如果是applicationId，可能有多个
////                            if (val instanceof List) {
////                                ((List) val).add(matchVal);
////                            } else {
////                                List l = new ArrayList<Object>();
////                                l.add(val);
////                                l.add(matchVal);
////                                val = l;
////                            }
////                            this.data.put(rule.getExtractLog().getName(), val);
////                        } else {
////                            this.data.put(rule.getExtractLog().getName(), matchVal);
////                        }
////                        hasApplicationIds = true;
//                    }
//                }
//            }
//
//            this.data.put(APPLICATION_ID, applicationIds);
//
//            if (applicationIds.size() > 0) {
//                return RetCode.RET_OK;
//            }

            // 可能没有日志
//            if (taskType.equals(TASK_TYPE_FLINK)) {
//                log.info("filePaths Count=" + filePaths.size() + ", hasContentCount=" + countFileIfHasContent);
//                return RetCode.RET_OP_NEED_RETRY;
//            }
//
//            return RetCode.RET_DATA_NOT_EXIST;
//        }

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
         * 获取数据
         */
//        public Map<String, Object> getData() {
//            return this.data;
//        }

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

    class ApplicationMessage{

        private Set<String> applicationIds = new HashSet<String>();
        private Set<String> logPaths = new HashSet<String>();

        public ApplicationMessage(){

        }

        public void addApplicationId(String appId){
            this.applicationIds.add(appId);
        }

        public void addLogPath(String logPath){
            this.logPaths.add(logPath);
        }

        public Set<String> getApplicationIds() {
            return applicationIds;
        }

        public Set<String> getLogPaths() {
            return logPaths;
        }
    }
}
