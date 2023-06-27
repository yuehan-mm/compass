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

package com.oppo.cloud.detect.detector;

import com.alibaba.fastjson2.JSONObject;
import com.oppo.cloud.common.domain.elasticsearch.JobAnalysis;
import com.oppo.cloud.common.domain.elasticsearch.SimpleUser;
import com.oppo.cloud.common.domain.job.LogRecord;
import com.oppo.cloud.common.service.RedisService;
import com.oppo.cloud.common.util.DateUtil;
import com.oppo.cloud.detect.domain.AbnormalTaskAppInfo;
import com.oppo.cloud.detect.service.*;
import com.oppo.cloud.detect.util.DetectorUtil;
import com.oppo.cloud.mapper.*;
import com.oppo.cloud.model.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 任务诊断父类
 */
@Slf4j
public abstract class DetectServiceImpl implements DetectService {

    @Autowired
    private ElasticSearchService elasticSearchService;

    @Value("${custom.elasticsearch.job-index}")
    private String jobIndex;

    // SPRING_KAFKA_TASKRECORD_TOPIC
    @Value("${spring.kafka.taskrecord.topic}")
    private String recordTopic;

    @Autowired
    public TaskService taskService;

    @Autowired
    private UserMapper userMapper;

    @Autowired
    private TaskAppService taskAppService;

    @Autowired
    private DelayedTaskService delayTaskService;

    @Autowired
    private AbnormalJobService abnormalJobService;

    @Autowired
    private RedisService redisService;

    @Autowired
    public TaskInstanceService taskInstanceService;

    @Autowired
    private JobInstanceService jobInstanceService;

    @Autowired
    private KafkaTemplate kafkaTemplate;

    /**
     * 解析消息发送redis队列
     */
    @Value("${custom.redis.logRecord}")
    private String logRecordQueue;

    /**
     * 任务诊断
     */
    @Override
    public abstract void detect(JobAnalysis detectJobAnalysis) throws Exception;

    /**
     * 正常作业任务处理
     */
    @Override
    public void handleNormalJob(JobAnalysis detectJobAnalysis, int tryNumber) throws Exception {
        // 补充用户信息
        updateUserInfo(detectJobAnalysis);

        // 解析task下面的Application信息
        AbnormalTaskAppInfo abnormalTaskAppInfo = taskAppService.getAbnormalTaskAppsInfo(detectJobAnalysis);

        if (!"".equals(abnormalTaskAppInfo.getExceptionInfo())) {
            // 完全构造完成再发送
            delayTaskService.pushDelayedQueue(detectJobAnalysis,abnormalTaskAppInfo.getExceptionInfo(), tryNumber);
            return;
        }

        // 引擎维度诊断必须要有appId
        if (abnormalTaskAppInfo.getTaskAppList().size() != 0) {
            abnormalJobService.updateResource(detectJobAnalysis, abnormalTaskAppInfo.getTaskAppList());
            // 生成解析日志消息体logRecord
            LogRecord logRecord = this.genLogRecord(abnormalTaskAppInfo, detectJobAnalysis);
            this.sendLogRecordMsg(logRecord);
        }
        // 保存jobInstance信息
        jobInstanceService.insertOrUpdate(detectJobAnalysis);
    }

    /**
     * 异常作业任务处理
     */
    @Override
    public void handleAbnormalJob(JobAnalysis detectJobAnalysis, int tryNumber) throws Exception {
        // 补充用户信息
        updateUserInfo(detectJobAnalysis);
        // 查询该任务下的appIds
        AbnormalTaskAppInfo abnormalTaskAppInfo = taskAppService.getAbnormalTaskAppsInfo(detectJobAnalysis);
        if (!"".equals(abnormalTaskAppInfo.getExceptionInfo())) {
            delayTaskService.pushDelayedQueue(detectJobAnalysis, abnormalTaskAppInfo.getExceptionInfo(), tryNumber);
            return;
        }
        // 引擎维度诊断必须要有appId
        if (abnormalTaskAppInfo.getTaskAppList().size() != 0) {
            taskAppService.insertTaskApps(abnormalTaskAppInfo.getTaskAppList());
            // 更新vcoreSeconds 和 memorySeconds
            abnormalJobService.updateResource(detectJobAnalysis, abnormalTaskAppInfo.getTaskAppList());
            // 生成解析日志消息体logRecord
            LogRecord logRecord = this.genLogRecord(abnormalTaskAppInfo, detectJobAnalysis);
            this.sendLogRecordMsg(logRecord);
        }
        // 保存jobInstance信息
        jobInstanceService.insertOrUpdate(detectJobAnalysis);
        // 保存异常任务
        this.addOrUpdate(detectJobAnalysis);
    }


    /**
     * 生成发送日志解析的消息体
     */
    public LogRecord genLogRecord(AbnormalTaskAppInfo abnormalTaskAppInfo, JobAnalysis detectJobAnalysis) {
        LogRecord logRecord = new LogRecord();
        logRecord.setId(UUID.randomUUID().toString());
        logRecord.setIsOneClick(false);
        logRecord.setJobAnalysis(detectJobAnalysis);
        logRecord.formatTaskAppList(abnormalTaskAppInfo.getTaskAppList());
//        List<App> appLogPath = logRecordService.getAppLog(abnormalTaskAppInfo.getTaskAppList());    // 转化SparkAppLog信息
//        List<App> schedulerLogApp = logRecordService.getSchedulerLog(detectJobAnalysis);            // 转化 AirFlow 日志信息
//        appLogPath.addAll(schedulerLogApp);
//        logRecord.setApps(appLogPath);     //
//        if (schedulerLogApp.size() != 0) {
//            // 更新已处理的事件信息【记录调度日志已成功发送】
//            abnormalTaskAppInfo.setHandleApps(abnormalTaskAppInfo.getHandleApps() + "scheduler" + ";");
//        }
        return logRecord;
    }

    /**
     */
    public void sendLogRecordMsg(LogRecord logRecord) {
        String recordMessage = JSONObject.toJSONString(logRecord);
        kafkaTemplate.send(recordTopic, recordMessage);
    }

    /**
     * 保存异常任务数据
     */
    public void addOrUpdate(JobAnalysis detectJobAnalysis) throws Exception {
        JobAnalysis esJobAnalysis = abnormalJobService.searchJob(detectJobAnalysis);

        if (esJobAnalysis != null) {
            // 更新操作
            List<String> oldCategories = esJobAnalysis.getCategories();
            oldCategories.addAll(detectJobAnalysis.getCategories());
            esJobAnalysis.setCategories(oldCategories.stream().distinct().collect(Collectors.toList()));
            if (Strings.isNotBlank(detectJobAnalysis.getSuccessExecutionDay())) {
                esJobAnalysis.setSuccessExecutionDay(detectJobAnalysis.getSuccessExecutionDay());
            }
            if (Strings.isBlank(detectJobAnalysis.getSuccessDays())) {
                esJobAnalysis.setSuccessDays(detectJobAnalysis.getSuccessDays());
            }
            if (Strings.isBlank(detectJobAnalysis.getDurationBaseline())) {
                esJobAnalysis.setDurationBaseline(detectJobAnalysis.getDurationBaseline());
            }
            if (Strings.isBlank(detectJobAnalysis.getEndTimeBaseline())) {
                esJobAnalysis.setEndTimeBaseline(detectJobAnalysis.getEndTimeBaseline());
            }
            esJobAnalysis.setUpdateTime(new Date());
            elasticSearchService.insertOrUpDateEs(esJobAnalysis.getIndex(), esJobAnalysis.getDocId(), esJobAnalysis.genDoc());
        } else {
            // 新增操作
            detectJobAnalysis.setCreateTime(new Date());
            detectJobAnalysis.setUpdateTime(new Date());
            String index = detectJobAnalysis.genIndex(jobIndex);
            String docId = detectJobAnalysis.genDocId();
            elasticSearchService.insertOrUpDateEs(index, docId, detectJobAnalysis.genDoc());
            // 记录索引信息和Id
            detectJobAnalysis.setIndex(index);
            detectJobAnalysis.setDocId(docId);
        }
    }


    /**
     * 补充任务的用户信息
     */
    public void updateUserInfo(JobAnalysis detectJobAnalysis) {

        Task task = taskService.getTask(detectJobAnalysis.getProjectName(),
                detectJobAnalysis.getFlowName(),
                detectJobAnalysis.getTaskName());

        if (task == null) {
            log.error("get task null:{}", detectJobAnalysis);
            return;
        }

        detectJobAnalysis.setTaskId(task.getId());
        detectJobAnalysis.setProjectId(task.getProjectId());
        detectJobAnalysis.setFlowId(task.getFlowId());

        UserExample userExample = new UserExample();
        userExample.createCriteria().andIdEqualTo(task.getUserId());
        List<User> users = userMapper.selectByExample(userExample);
        if (users.size() > 0) {
            User user = users.get(0);
            SimpleUser simpleUser = new SimpleUser();
            simpleUser.setUserId(user.getUserId());
            simpleUser.setUsername(user.getUsername());
            detectJobAnalysis.setUsers(Collections.singletonList(simpleUser));
        }

    }

    public double[] getEndTimeBaseline(JobAnalysis detectJobAnalysis) throws Exception {
        Date startTime = DateUtil.getOffsetDate(detectJobAnalysis.getExecutionDate(), -30);
        // 获取近一个月内的数据
        List<Double> relativeEndDateHistory = taskInstanceService.searchTaskRelativeEndTime(
                detectJobAnalysis.getProjectName(), detectJobAnalysis.getFlowName(),
                detectJobAnalysis.getTaskName(), detectJobAnalysis.getExecutionDate(), startTime, 20);
        // 样本值小于10的，不进行异常检测
        if (relativeEndDateHistory.size() < 10) {
            return null;
        }
        Double[] relativeEndDate = relativeEndDateHistory.toArray(new Double[0]);
        long executionTimestamp = detectJobAnalysis.getExecutionDate().getTime() / 1000;
        double[] relativeMedian = DetectorUtil.boxplotValue(relativeEndDate);
        double relativeEndDateStart = relativeMedian[0];
        double relativeEndDateEnd = relativeMedian[4];
        double normalEndDateBegin = relativeEndDateStart + executionTimestamp;
        double normalEndDateEnd = relativeEndDateEnd + executionTimestamp;
        return new double[]{normalEndDateBegin, normalEndDateEnd};
    }

    public double[] getDurationBaseline(JobAnalysis detectJobAnalysis) throws Exception {
        Date startTime = DateUtil.getOffsetDate(detectJobAnalysis.getExecutionDate(), 30);
        // 查询近一个月的历史数据
        List<Double> durationHistory = taskInstanceService.searchTaskDurationHistory(detectJobAnalysis.getProjectName(),
                detectJobAnalysis.getFlowName(), detectJobAnalysis.getTaskName(), detectJobAnalysis.getExecutionDate(),
                startTime, 20);
        // 样本值小于10的，不进行异常检测
        if (durationHistory.size() < 10) {
            return null;
        }
        Double[] durationData = durationHistory.toArray(new Double[0]);
        // 箱线图法
        double[] durationBeginAndEnd = DetectorUtil.boxplotValue(durationData);
        // 极端异常值
        double normalDurationBegin = durationBeginAndEnd[0];
        double normalDurationEnd = durationBeginAndEnd[4];
        return new double[]{normalDurationBegin, normalDurationEnd};
    }
}
