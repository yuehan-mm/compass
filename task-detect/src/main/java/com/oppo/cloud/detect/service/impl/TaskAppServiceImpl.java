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

package com.oppo.cloud.detect.service.impl;

import com.oppo.cloud.common.domain.elasticsearch.JobAnalysis;
import com.oppo.cloud.common.domain.elasticsearch.TaskApp;
import com.oppo.cloud.common.service.RedisService;
import com.oppo.cloud.common.util.DateUtil;
import com.oppo.cloud.common.util.ui.TryNumberUtil;
import com.oppo.cloud.detect.domain.AbnormalTaskAppInfo;
import com.oppo.cloud.detect.handler.app.*;
import com.oppo.cloud.detect.service.ElasticSearchService;
import com.oppo.cloud.detect.service.TaskAppService;
import com.oppo.cloud.mapper.TaskApplicationMapper;
import com.oppo.cloud.model.TaskApplication;
import com.oppo.cloud.model.TaskApplicationExample;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 异常任务App接口类
 */
@Service
@Slf4j
public class TaskAppServiceImpl implements TaskAppService {

    @Autowired
    private TaskApplicationMapper taskApplicationMapper;

    @Autowired
    private ElasticSearchService elasticSearchService;

    @Autowired
    private RedisService redisService;

    /**
     * sparkUi代理前缀
     */
    @Value("${custom.sparkUiProxy.url}")
    private String sparkUiProxy;

    @Value("${custom.elasticsearch.app-index}")
    private String appIndex;

    @Value("${custom.schedulerType}")
    private String schedulerType;

    @Value("${spring.hdfs.base-path}")
    private String hdfsBasePath;

    @Override
    public AbnormalTaskAppInfo getTaskAppsInfo(JobAnalysis jobAnalysis) {
        AbnormalTaskAppInfo abnormalTaskAppInfo = new AbnormalTaskAppInfo();
        List<TaskApp> taskAppList = new ArrayList<>();
        // 收集每个appId的异常信息
        StringBuilder exceptionInfo = new StringBuilder();
        List<TaskApplication> taskApplicationList = getTaskApplications(jobAnalysis);
        if (!taskApplicationList.isEmpty()) {
            // 遍历所有的Application信息
            for (TaskApplication taskApplication : taskApplicationList) {
                // 校正重试次数
                taskApplication.setRetryTimes(TryNumberUtil.updateTryNumber(taskApplication.getRetryTimes(), schedulerType));
                try {
                    // 根据appId构造TaskApp(包括相关的日志路径)
                    TaskApp taskApp = this.buildTaskApp(taskApplication);
                    // 将元数据信息更新到taskApp中
                    taskApp.setTaskId(jobAnalysis.getTaskId());
                    taskApp.setFlowId(jobAnalysis.getFlowId());
                    taskApp.setProjectId(jobAnalysis.getProjectId());
                    taskApp.setUsers(jobAnalysis.getUsers());
                    taskAppList.add(taskApp);
                } catch (Exception e) {
                    exceptionInfo.append(e.getMessage()).append(";");
                }
            }
        } else {
            exceptionInfo.append("can not find TaskApplication info.");
        }

        abnormalTaskAppInfo.setTaskAppList(taskAppList);                  // Application 信息
        abnormalTaskAppInfo.setExceptionInfo(exceptionInfo.toString());   // 解析过程中的异常信息
        return abnormalTaskAppInfo;
    }


    @Override
    public List<TaskApp> searchTaskApps(JobAnalysis jobAnalysis) throws Exception {
        HashMap<String, Object> termCondition = new HashMap<>();
        termCondition.put("projectName.keyword", jobAnalysis.getProjectName());
        termCondition.put("flowName.keyword", jobAnalysis.getFlowName());
        termCondition.put("taskName.keyword", jobAnalysis.getTaskName());
        termCondition.put("executionDate", DateUtil.timestampToUTCDate(jobAnalysis.getExecutionDate().getTime()));
        SearchSourceBuilder searchSourceBuilder = elasticSearchService.genSearchBuilder(termCondition, null, null, null);
        return elasticSearchService.find(TaskApp.class, searchSourceBuilder, appIndex + "-*");
    }

    /**
     * 根据基础的appId信息构建出AbnormalTaskApp,有异常则直接退出抛出异常
     */
    public TaskApp buildTaskApp(TaskApplication taskApplication) {
        TaskApp taskApp = new TaskApp();
        BeanUtils.copyProperties(taskApplication, taskApp);
        taskApp.setApplicationId(taskApp.getApplicationId());
        taskApp.setExecutionDate(taskApplication.getExecuteTime());
        taskApp.setRetryTimes(taskApplication.getRetryTimes());
        buildTaskApp(taskApplication, taskApp);
        return taskApp;
    }

    public void buildTaskApp(TaskApplication taskApplication, TaskApp taskApp) {
        try {
            switch (taskApplication.getApplicationType()) {
                case SPARK:
                    new SparkTaskAppHandler().handler(taskApplication, taskApp, elasticSearchService, redisService);
                    break;
                case MAPREDUCE:
                    new MRTaskAppHandler().handler(taskApplication, taskApp, elasticSearchService, redisService);
                    break;
                case DATAX:
                    new DataXTaskAppHandler().handler(taskApplication, taskApp, hdfsBasePath);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid taskApp type : " + taskApplication.getApplicationType());
            }
        } catch (Exception e) {
            log.error("try complete yarn info failed, msg:", e);
            throw new RuntimeException(e.getMessage());
        }
    }


    public List<TaskApplication> getTaskApplications(JobAnalysis jobAnalysis) {
        TaskApplicationExample taskApplicationExample = new TaskApplicationExample();
        taskApplicationExample.createCriteria()
                .andProjectNameEqualTo(jobAnalysis.getProjectName())
                .andFlowNameEqualTo(jobAnalysis.getFlowName())
                .andTaskNameEqualTo(jobAnalysis.getTaskName())
                .andExecuteTimeEqualTo(jobAnalysis.getExecutionDate())
                .andRetryTimesEqualTo(jobAnalysis.getRetryTimes() + 1)
                .andApplicationIdIsNotNull();
        return taskApplicationMapper.selectByExample(taskApplicationExample);
    }
}
