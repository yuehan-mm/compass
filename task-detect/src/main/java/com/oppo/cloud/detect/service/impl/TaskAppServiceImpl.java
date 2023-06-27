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

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import com.oppo.cloud.common.constant.AppCategoryEnum;
import com.oppo.cloud.common.constant.Constant;
import com.oppo.cloud.common.domain.cluster.spark.SparkApp;
import com.oppo.cloud.common.domain.cluster.yarn.YarnApp;
import com.oppo.cloud.common.domain.elasticsearch.JobAnalysis;
import com.oppo.cloud.common.domain.elasticsearch.TaskApp;
import com.oppo.cloud.common.service.RedisService;
import com.oppo.cloud.common.util.DateUtil;
import com.oppo.cloud.common.util.ui.TryNumberUtil;
import com.oppo.cloud.detect.domain.AbnormalTaskAppInfo;
import com.oppo.cloud.detect.handler.app.TaskAppHandler;
import com.oppo.cloud.detect.handler.app.TaskAppHandlerFactory;
import com.oppo.cloud.detect.service.*;
import com.oppo.cloud.detect.util.AppNotFoundException;
import com.oppo.cloud.mapper.TaskApplicationMapper;
import com.oppo.cloud.model.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.*;

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


    @Override
    public AbnormalTaskAppInfo getAbnormalTaskAppsInfo(JobAnalysis jobAnalysis) {
        AbnormalTaskAppInfo abnormalTaskAppInfo = new AbnormalTaskAppInfo();
        List<TaskApp> taskAppList = new ArrayList<>();
        // 收集每个appId的异常信息
        StringBuilder exceptionInfo = new StringBuilder();
        // 判断任务每次重试的appId是否已经找到
        Map<Integer, Boolean> needed = new HashMap<>();
        for (int i = 0; i <= jobAnalysis.getRetryTimes(); i++) {
            needed.put(i, false);
        }

        // 获取到 Yarn application Id
        // 历史所有重试过的 ??
        List<TaskApplication> taskApplicationList = getTaskApplications(jobAnalysis.getProjectName(),
                jobAnalysis.getFlowName(),
                jobAnalysis.getTaskName(),
                jobAnalysis.getExecutionDate()
        );

        // 遍历所有的Application信息
        for (TaskApplication taskApplication : taskApplicationList) {
            // 校正重试次数
            taskApplication.setRetryTimes(TryNumberUtil.updateTryNumber(taskApplication.getRetryTimes(), schedulerType));

            try {
                if (needed.containsKey(taskApplication.getRetryTimes())) {
                    needed.put(taskApplication.getRetryTimes(), true);
                } else {
                    // 兼容手动执行的任务，所有的重试当成不同周期的第一次重试
                    taskApplication.setRetryTimes(0);
                    needed.put(0, true);
                }
                // 根据appId构造TaskApp(包括相关的日志路径)
                TaskApp taskApp = this.buildAbnormalTaskApp(taskApplication);
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
        List<String> notFound = new ArrayList<>();
        // 标志已经查询到的appId的重试次数
        for (Integer taskTryNum : needed.keySet()) {
            if (!needed.get(taskTryNum)) {
                notFound.add(String.valueOf(taskTryNum));
            }
        }
        // 判断是否查到所有重试次数下的appId
        if (notFound.size() > 0) {
            exceptionInfo.append(String.format("can not find appId by tryNum: %s", String.join(",", notFound)));
        }

        abnormalTaskAppInfo.setTaskAppList(taskAppList);                  // Application 信息
        abnormalTaskAppInfo.setExceptionInfo(exceptionInfo.toString());   // 解析过程中的异常信息
        return abnormalTaskAppInfo;
    }

    /**
     * 获取异常任务下的appId,包括有或没有的
     */

    @Override
    public Map<Integer, List<TaskApp>> getAbnormalTaskApps(JobAnalysis jobAnalysis) {
        Map<Integer, List<TaskApp>> res = new HashMap<>();
        List<TaskApplication> taskApplicationList = getTaskApplications(jobAnalysis.getProjectName(),
                jobAnalysis.getFlowName(), jobAnalysis.getTaskName(), jobAnalysis.getExecutionDate());
        // 根据重试次数构建出所有的重试记录
        for (int i = 0; i <= jobAnalysis.getRetryTimes(); i++) {
            List<TaskApp> temp = new ArrayList<>();
            res.put(i, temp);
        }
        // 查询出来所有的appIds
        for (TaskApplication taskApplication : taskApplicationList) {
            TaskApp taskApp = this.tryBuildAbnormalTaskApp(taskApplication);
            List<TaskApp> temp;
            if (res.containsKey(taskApplication.getRetryTimes())) {
                temp = res.get(taskApplication.getRetryTimes());
                temp.add(taskApp);
                res.put(taskApplication.getRetryTimes(), temp);
            } else {
                // 不包含则将这个appId放在第一次重试中
                temp = new ArrayList<>();
                taskApp.setRetryTimes(0);
                temp.add(taskApp);
                res.put(0, temp);
            }
        }
        // 没有appId的构造为空的
        for (Integer tryTime : res.keySet()) {
            List<TaskApp> temp = res.get(tryTime);
            if (temp.size() == 0) {
                TaskApp taskApp = new TaskApp();
                taskApp.setRetryTimes(tryTime);
                taskApp.setTaskName(jobAnalysis.getTaskName());
                taskApp.setFlowName(jobAnalysis.getFlowName());
                taskApp.setProjectName(jobAnalysis.getProjectName());
                taskApp.setExecutionDate(jobAnalysis.getExecutionDate());
                temp.add(taskApp);
                res.put(tryTime, temp);
            }
        }
        return res;
    }

    @Override
    public void insertTaskApps(List<TaskApp> taskAppList) throws Exception {
        for (TaskApp taskApp : taskAppList) {
            String index = taskApp.genIndex(appIndex);
            log.info("insertTaskApp {},{},{}", index, taskApp.getApplicationId(), taskApp.genDoc());
            elasticSearchService.insertOrUpDateEs(index, taskApp.genDocId(), taskApp.genDoc());
        }
    }

    @Override
    public List<TaskApp> searchTaskApps(JobAnalysis jobAnalysis) throws Exception {
        HashMap<String, Object> termCondition = new HashMap<>();
        termCondition.put("projectName.keyword", jobAnalysis.getProjectName());
        termCondition.put("flowName.keyword", jobAnalysis.getFlowName());
        termCondition.put("taskName.keyword", jobAnalysis.getTaskName());
        termCondition.put("executionDate", DateUtil.timestampToUTCDate(jobAnalysis.getExecutionDate().getTime()));
        SearchSourceBuilder searchSourceBuilder =
                elasticSearchService.genSearchBuilder(termCondition, null, null, null);
        return elasticSearchService.find(TaskApp.class, searchSourceBuilder, appIndex + "-*");
    }

    /**
     * 根据基础的appId信息构建出AbnormalTaskApp,有异常则直接退出抛出异常
     */
    public TaskApp buildAbnormalTaskApp(TaskApplication taskApplication) throws Exception {
        TaskApp taskApp = new TaskApp();
        BeanUtils.copyProperties(taskApplication, taskApp);
        taskApp.setApplicationId(taskApp.getApplicationId());
        taskApp.setExecutionDate(taskApplication.getExecuteTime());
        taskApp.setRetryTimes(taskApplication.getRetryTimes());

        final TaskAppHandler taskAppHandler = TaskAppHandlerFactory.getTaskAppHandler(taskApplication);

        taskAppHandler.handler(taskApplication, taskApp, elasticSearchService, redisService);

        return taskApp;
    }

    public TaskApp tryBuildAbnormalTaskApp(TaskApplication taskApplication) {
        TaskApp taskApp = new TaskApp();
        BeanUtils.copyProperties(taskApplication, taskApp);
        taskApp.setExecutionDate(taskApplication.getExecuteTime());
        taskApp.setRetryTimes(taskApplication.getRetryTimes());

        final TaskAppHandler taskAppHandler = TaskAppHandlerFactory.getTaskAppHandler(taskApplication);

        try {
            taskAppHandler.handler(taskApplication, taskApp, elasticSearchService, redisService);
        } catch (Exception e) {
            log.error("try complete yarn info failed, msg:", e);
        }

        return taskApp;
    }



    public List<TaskApplication> getTaskApplications(String projectName, String flowName, String taskName,
                                                     Date executionTime) {
        TaskApplicationExample taskApplicationExample = new TaskApplicationExample();
        taskApplicationExample.createCriteria()
                .andProjectNameEqualTo(projectName)
                .andFlowNameEqualTo(flowName)
                .andTaskNameEqualTo(taskName)
                .andExecuteTimeEqualTo(executionTime)
                .andApplicationIdIsNotNull();
        return taskApplicationMapper.selectByExample(taskApplicationExample);
    }
}
