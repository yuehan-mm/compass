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

package com.oppo.cloud.parser.utils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.oppo.cloud.common.constant.ApplicationType;
import com.oppo.cloud.parser.domain.mapreduce.eventlog.JobFinishedEvent;
import com.oppo.cloud.parser.domain.mapreduce.eventlog.MapReduceApplication;
import com.oppo.cloud.parser.domain.spark.eventlog.*;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * spark event log 解析
 */
@Slf4j
@Data
public class ReplaySparkEventLogs extends ReplayEventLogs {

    private SparkApplication application;
    private Map<Integer, SparkJob> jobs;
    private Map<Integer, Long> jobSQLExecIDMap;
    private Map<String, SparkExecutor> executors;
    private Map<Long, SparkTask> tasks;
    private List<SparkBlockManager> blockManagers;
    private List<StageInfo> failedStages;
    private List<SparkListenerDriverAccumUpdates> driverAccumUpdates;
    private List<SparkListenerSQLExecutionStart> sqlExecutionStarts;
    private Map<Long, AccumulableInfo> accumulableInfoMap;
    private Map<Long, Long> driverUpdateMap;
    private List<String> rawSQLExecutions;
    private ObjectMapper objectMapper;
    private Map<Integer, Integer> stageIDToJobID;
    private long logSize;


    public ReplaySparkEventLogs(ApplicationType applicationType, String logPath) {
        super(applicationType, logPath);
        application = new SparkApplication();
        jobs = new HashMap<>();
        jobSQLExecIDMap = new HashMap<>();
        executors = new HashMap<>();
        tasks = new HashMap<>();
        blockManagers = new ArrayList<>();
        failedStages = new ArrayList<>();
        driverAccumUpdates = new ArrayList<>();
        sqlExecutionStarts = new ArrayList<>();
        accumulableInfoMap = new HashMap<>();
        driverUpdateMap = new HashMap<>();
        rawSQLExecutions = new ArrayList<>();
        stageIDToJobID = new HashMap<>();
        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /**
     * 按行解析
     */
    @Override
    public void parseLine(String line) throws Exception {
        SparkListenerEvent event = objectMapper.readValue(line, SparkListenerEvent.class);
        switch (event.getEvent()) {
            case "SparkListenerApplicationStart":
                SparkListenerApplicationStart sparkListenerApplicationStart = objectMapper.readValue(line,
                        SparkListenerApplicationStart.class);
                this.application.setAppStartTimestamp(sparkListenerApplicationStart.getTime());
                break;
            case "SparkListenerApplicationEnd":
                SparkListenerApplicationEnd sparkListenerApplicationEnd = objectMapper.readValue(line,
                        SparkListenerApplicationEnd.class);
                this.application.setAppEndTimestamp(sparkListenerApplicationEnd.getTime());
                break;
            case "SparkListenerBlockManagerAdded":
                SparkListenerBlockManagerAdded sparkListenerBlockManagerAdded = objectMapper.readValue(line,
                        SparkListenerBlockManagerAdded.class);
                SparkBlockManager blockManager = new SparkBlockManager(sparkListenerBlockManagerAdded);
                this.blockManagers.add(blockManager);
                break;
            case "SparkListenerBlockManagerRemoved":
                SparkListenerBlockManagerRemoved sparkListenerBlockManagerRemoved = objectMapper.readValue(line,
                        SparkListenerBlockManagerRemoved.class);
                break;
            case "org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates":
                SparkListenerDriverAccumUpdates sparkListenerDriverAccumUpdates = objectMapper.readValue(line,
                        SparkListenerDriverAccumUpdates.class);
                this.driverAccumUpdates.add(sparkListenerDriverAccumUpdates);
                break;
            case "SparkListenerEnvironmentUpdate":
                SparkListenerEnvironmentUpdate sparkListenerEnvironmentUpdate = objectMapper.readValue(line,
                        SparkListenerEnvironmentUpdate.class);
                this.application.setSparkApplication(sparkListenerEnvironmentUpdate);
                break;
            case "SparkListenerExecutorAdded":
                SparkListenerExecutorAdded sparkListenerExecutorAdded = objectMapper.readValue(line,
                        SparkListenerExecutorAdded.class);
                String id = sparkListenerExecutorAdded.getExecutorId();
                this.executors.put(id, new SparkExecutor(sparkListenerExecutorAdded));
                break;
            case "SparkListenerExecutorRemoved":
                SparkListenerExecutorRemoved sparkListenerExecutorRemoved = objectMapper.readValue(line,
                        SparkListenerExecutorRemoved.class);
                String execId = sparkListenerExecutorRemoved.getExecutorId();
                this.executors.get(execId).remove(sparkListenerExecutorRemoved);
                break;
            case "SparkListenerJobEnd":
                SparkListenerJobEnd sparkListenerJobEnd = objectMapper.readValue(line,
                        SparkListenerJobEnd.class);
                this.jobs.get(sparkListenerJobEnd.getJobId()).complete(sparkListenerJobEnd);
                break;
            case "SparkListenerJobStart":
                SparkListenerJobStart sparkListenerJobStart = objectMapper.readValue(line,
                        SparkListenerJobStart.class);
                if (this.jobs.get(sparkListenerJobStart.getJobId()) != null) {
                    log.error("ERROR: Duplicate job ID:{}", sparkListenerJobStart.getJobId());
                    break;
                }
                SparkJob job = new SparkJob(sparkListenerJobStart);
                this.jobs.put(job.getJobId(), job);
                // jobId sql.execution.id关系映射
                String sqlExecutionID = sparkListenerJobStart.getProperties().getProperty("spark.sql.execution" +
                        ".id");
                if (!StringUtils.isEmpty(sqlExecutionID)) {
                    jobSQLExecIDMap.put(job.getJobId(), Long.valueOf(sqlExecutionID));
                }
                // stateId jobId关系映射
                if (sparkListenerJobStart.getStageInfos() != null) {
                    sparkListenerJobStart.getStageInfos().forEach(stageInfo -> {
                        stageIDToJobID.put(stageInfo.getStageId(), sparkListenerJobStart.getJobId());
                    });
                }

                break;
            case "SparkListenerLogStart":
                SparkListenerLogStart sparkListenerLogStart = objectMapper.readValue(line,
                        SparkListenerLogStart.class);
                // this.application.setSparkVersion(sparkListenerLogStart.getSparkVersion());
                break;
            case "SparkListenerSQLAdaptiveExecutionUpdate":
                SparkListenerSQLAdaptiveExecutionUpdate sparkListenerSQLAdaptiveExecutionUpdate =
                        objectMapper.readValue(line, SparkListenerSQLAdaptiveExecutionUpdate.class);
                break;
            case "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd":
                SparkListenerSQLExecutionEnd sparkListenerSQLExecutionEnd = objectMapper.readValue(line,
                        SparkListenerSQLExecutionEnd.class);
                rawSQLExecutions.add(line);
                break;
            case "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart":
                SparkListenerSQLExecutionStart sparkListenerSQLExecutionStart = objectMapper.readValue(line,
                        SparkListenerSQLExecutionStart.class);
                sqlExecutionStarts.add(sparkListenerSQLExecutionStart);
                rawSQLExecutions.add(line);
                break;
            case "SparkListenerStageCompleted":
                SparkListenerStageCompleted sparkListenerStageCompleted = objectMapper.readValue(line,
                        SparkListenerStageCompleted.class);
                Integer stageId = sparkListenerStageCompleted.getStageInfo().getStageId();
                for (SparkJob sparkJob : this.jobs.values()) {
                    for (SparkStage stage : sparkJob.getStages()) {
                        if (stage.getStageId().equals(stageId)) {
                            stage.complete(sparkListenerStageCompleted);
                        }
                    }
                }
                StageInfo stageInfo = sparkListenerStageCompleted.getStageInfo();
                if (stageInfo != null && stageInfo.getFailureReason() != null
                        && !stageInfo.getFailureReason().isEmpty()) {
                    this.getFailedStages().add(stageInfo);
                }
                break;
            case "SparkListenerStageSubmitted":
                SparkListenerStageSubmitted sparkListenerStageSubmitted = objectMapper.readValue(line,
                        SparkListenerStageSubmitted.class);
                break;
            case "SparkListenerTaskEnd":
                SparkListenerTaskEnd sparkListenerTaskEnd = objectMapper.readValue(line,
                        SparkListenerTaskEnd.class);
                Long taskId = sparkListenerTaskEnd.getTaskInfo().getTaskId();
                this.tasks.get(taskId).finish(sparkListenerTaskEnd);
                // 更新job数据
                Integer jobID = stageIDToJobID.get(sparkListenerTaskEnd.getStageId());
                if (jobID != null && sparkListenerTaskEnd.getTaskMetrics() != null) {
                    SparkJob sparkJob = jobs.get(jobID);
                    if (sparkJob != null) {
                        Long sum = sparkJob.getExecutorRunTime() +
                                sparkListenerTaskEnd.getTaskMetrics().getExecutorRunTime();
                        sparkJob.setExecutorRunTime(sum);
                    }
                }
                break;
            case "SparkListenerTaskGettingResult":
                SparkListenerTaskGettingResult sparkListenerTaskGettingResult = objectMapper.readValue(line,
                        SparkListenerTaskGettingResult.class);
                break;
            case "SparkListenerTaskStart":
                SparkListenerTaskStart sparkListenerTaskStart = objectMapper.readValue(line,
                        SparkListenerTaskStart.class);
                taskId = sparkListenerTaskStart.getTaskInfo().getTaskId();
                this.tasks.put(taskId, new SparkTask(sparkListenerTaskStart));
                break;
            default:
                break;
        }

    }


    /**
     * 相关数据处理
     */
    @Override
    public void correlate() {
        for (SparkBlockManager blockManager : this.blockManagers) {
            if (!"driver".equals(blockManager.getExecutorId())) {
                SparkExecutor executor = this.executors.get(blockManager.getExecutorId());
                if (executor != null) {
                    executor.getBlockManagers().add(blockManager);
                }
            }
        }

        for (SparkTask task : this.tasks.values()) {
            this.executors.get(task.getExecutorId()).getTasks().add(task);
            for (SparkJob job : this.jobs.values()) {
                for (SparkStage stage : job.getStages()) {
                    // 重试stageId映射
                    for (Map.Entry<Integer, Long> entry : stage.getSubmissionTimeMap().entrySet()) {
                        Integer attemptId = entry.getKey();
                        if (stage.getStageId().equals(task.getStageId())
                                && attemptId.equals(task.getStageAttemptId())) {
                            if (stage.getTasksMap().containsKey(attemptId)) {
                                stage.getTasksMap().get(attemptId).add(task);
                            } else {
                                List<SparkTask> sparkTasks = new ArrayList<>();
                                sparkTasks.add(task);
                                stage.getTasksMap().put(attemptId, sparkTasks);
                            }
                        }
                    }
                }
            }
        }
        updateAccum();
    }

    /**
     * update AccumulableInfo driverUpdateMap
     */
    private void updateAccum() {
        for (SparkJob job : this.jobs.values()) {
            for (SparkStage stage : job.getStages()) {
                if (stage.getAccumulableInfos() != null) {
                    for (AccumulableInfo info : stage.getAccumulableInfos()) {
                        this.accumulableInfoMap.put(info.getId(), info);
                    }
                }
            }
        }
        for (SparkListenerDriverAccumUpdates updates : this.driverAccumUpdates) {
            for (List<Long> update : updates.getAccumUpdates()) {
                this.driverUpdateMap.put(update.get(0), update.get(1));
            }
        }
    }

    @Override
    public JobFinishedEvent getJobFinishedEvent() {
        return null;
    }

    @Override
    public MapReduceApplication getMapReduceApplication() {
        return null;
    }
}
