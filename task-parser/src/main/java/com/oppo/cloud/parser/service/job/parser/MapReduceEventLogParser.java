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

package com.oppo.cloud.parser.service.job.parser;

import com.alibaba.fastjson2.JSON;
import com.oppo.cloud.common.constant.ProgressState;
import com.oppo.cloud.common.domain.eventlog.DetectorStorage;
import com.oppo.cloud.common.domain.eventlog.config.DetectorConfig;
import com.oppo.cloud.common.domain.eventlog.config.SparkEnvironmentConfig;
import com.oppo.cloud.common.domain.job.LogPath;
import com.oppo.cloud.common.domain.oneclick.OneClickProgress;
import com.oppo.cloud.common.domain.oneclick.ProgressInfo;
import com.oppo.cloud.common.util.spring.SpringBeanUtil;
import com.oppo.cloud.parser.domain.job.*;
import com.oppo.cloud.parser.domain.reader.ReaderObject;
import com.oppo.cloud.parser.domain.spark.eventlog.SparkApplication;
import com.oppo.cloud.parser.domain.spark.eventlog.SparkExecutor;
import com.oppo.cloud.parser.service.job.detector.DetectorManager;
import com.oppo.cloud.parser.service.job.oneclick.OneClickSubject;
import com.oppo.cloud.parser.service.reader.IReader;
import com.oppo.cloud.parser.service.reader.LogReaderFactory;
import com.oppo.cloud.parser.service.rules.JobRulesConfigService;
import com.oppo.cloud.parser.utils.ReplayEventLogs;
import lombok.extern.slf4j.Slf4j;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class MapReduceEventLogParser extends OneClickSubject implements IParser {

    private final ParserParam param;

    private DetectorConfig config;

    private boolean isOneClick;

    public MapReduceEventLogParser(ParserParam param) {
        this.param = param;
        JobRulesConfigService jobRulesConfigService = (JobRulesConfigService) SpringBeanUtil.getBean(JobRulesConfigService.class);
        this.config = jobRulesConfigService.detectorConfig;
        this.isOneClick = param.getTaskParam().getIsOneClick();
    }

    @Override
    public CommonResult run() {
        updateParserProgress(ProgressState.PROCESSING, 0, this.param.getLogPaths().size());
        if (this.param.getLogPaths().size() > 0) {
            LogPath logPath = this.param.getLogPaths().get(0);
            ReaderObject readerObjects;
            try {
                IReader reader = LogReaderFactory.create(logPath);
                reader.setMapReduceEventLogPath();
                log.info("update ReduceEventLogPath : " + reader.getReaderObject().getLogPath());
                readerObjects = reader.getReaderObject();
            } catch (FileNotFoundException e) {
                String path = logPath.getLogPath().substring(0, logPath.getLogPath().lastIndexOf("_"));
                logPath.setLogPath(path);
                try {
                    readerObjects = LogReaderFactory.create(logPath).getReaderObject();
                } catch (Exception ex) {
                    log.error("MapReduceEventLogParser fail:" + e.getMessage());
                    updateParserProgress(ProgressState.FAILED, 0, 0);
                    return null;
                }
            } catch (Exception e) {
                log.error("Exception:", e);
                updateParserProgress(ProgressState.FAILED, 0, 0);
                return null;
            }
            return parse(readerObjects);
        }
        return null;
    }

    private CommonResult<MapReduceEventLogParserResult> parse(ReaderObject readerObject) {
        ReplayEventLogs replayEventLogs = new ReplayEventLogs(this.param.getTaskParam().getTaskApp().getApplicationType());
        try {
            replayEventLogs.replay(readerObject);
        } catch (Exception e) {
            log.error("Exception:", e);
            updateParserProgress(ProgressState.FAILED, 0, 0);
            return null;
        }
        log.info("replay result: " + JSON.toJSONString(replayEventLogs));
        return detect(replayEventLogs, readerObject.getLogPath());
    }

    private CommonResult<MapReduceEventLogParserResult> detect(ReplayEventLogs replayEventLogs, String logPath) {
//        Map<String, Object> env = getSparkEnvironmentConfig(replayEventLogs);

//        Long appDuration = replayEventLogs.getApplication().getAppDuration();
//        if (appDuration == null || appDuration < 0) {
//            appDuration = 0L;
//        }

        DetectorParam detectorParam = new DetectorParam(this.param.getTaskParam().getTaskApp().getFlowName(),
                this.param.getTaskParam().getTaskApp().getProjectName(),
                this.param.getTaskParam().getTaskApp().getTaskName(),
                this.param.getTaskParam().getTaskApp().getExecutionDate(),
                this.param.getTaskParam().getTaskApp().getRetryTimes(),
                this.param.getTaskParam().getTaskApp().getApplicationId(),
                this.param.getTaskParam().getTaskApp().getApplicationType(),
                0l, logPath, config, replayEventLogs,
                isOneClick);

        DetectorManager detectorManager = new DetectorManager(detectorParam);
        // run all detector
        DetectorStorage detectorStorage = detectorManager.run();

//        detectorStorage.setEnv(env);
        MapReduceEventLogParserResult mapReduceEventLogParserResult = new MapReduceEventLogParserResult();
        mapReduceEventLogParserResult.setDetectorStorage(detectorStorage);
//        mapReduceEventLogParserResult.setMemoryCalculateParam(getMemoryCalculateParam(replayEventLogs));

        CommonResult<MapReduceEventLogParserResult> result = new CommonResult<>();
        result.setLogType(this.param.getLogType());
        result.setResult(mapReduceEventLogParserResult);

        updateParserProgress(ProgressState.SUCCEED, 0, 0);
        return result;
    }

    private Map<String, Object> getSparkEnvironmentConfig(ReplayEventLogs replayEventLogs) {
        Map<String, Object> env = new HashMap<>();
        SparkEnvironmentConfig envConfig = config.getSparkEnvironmentConfig();
        if (envConfig != null) {
            if (envConfig.getJvmInformation() != null) {
                for (String key : envConfig.getJvmInformation()) {
                    env.put(key, replayEventLogs.getApplication().getJvmInformation().get(key));
                }
            }
            if (envConfig.getSparkProperties() != null) {
                for (String key : envConfig.getSparkProperties()) {
                    env.put(key, replayEventLogs.getApplication().getSparkProperties().get(key));
                }
            }
            if (envConfig.getSystemProperties() != null) {
                for (String key : envConfig.getSystemProperties()) {
                    env.put(key, replayEventLogs.getApplication().getSystemProperties().get(key));
                }
            }
        }
        return env;
    }


    public MemoryCalculateParam getMemoryCalculateParam(ReplayEventLogs replayEventLogs) {
        SparkApplication application = replayEventLogs.getApplication();
        long appTotalTime = application.getAppEndTimestamp() - application.getAppStartTimestamp();
        MemoryCalculateParam memoryCalculateParam = new MemoryCalculateParam();
        memoryCalculateParam.setAppTotalTime(appTotalTime > 0 ? appTotalTime : 0);
        memoryCalculateParam.setDriverMemory(application.getDriverMemory());
        memoryCalculateParam.setExecutorMemory(application.getExecutorMemory());

        Map<String, Long> executorRuntimeMap = new HashMap<>();
        for (Map.Entry<String, SparkExecutor> executor : replayEventLogs.getExecutors().entrySet()) {
            SparkExecutor sparkExecutor = executor.getValue();
            long endTime = sparkExecutor.getRemoveTimestamp() > 0 ? sparkExecutor.getRemoveTimestamp()
                    : application.getAppEndTimestamp();

            long startTime = sparkExecutor.getStartTimestamp() > 0 ? sparkExecutor.getStartTimestamp()
                    : application.getAppStartTimestamp();

            long executorRuntime = endTime - startTime;
            executorRuntimeMap.put(executor.getValue().getId(), executorRuntime);
        }
        memoryCalculateParam.setExecutorRuntimeMap(executorRuntimeMap);
        return memoryCalculateParam;
    }


    public void updateParserProgress(ProgressState state, Integer progress, Integer count) {
        if (!this.isOneClick) {
            return;
        }
        OneClickProgress oneClickProgress = new OneClickProgress();
        oneClickProgress.setAppId(this.param.getTaskParam().getTaskApp().getApplicationId());
        oneClickProgress.setLogType(this.param.getLogType());
        ProgressInfo executorProgress = new ProgressInfo();
        executorProgress.setCount(count);
        executorProgress.setProgress(progress);
        executorProgress.setState(state);
        oneClickProgress.setProgressInfo(executorProgress);
        update(oneClickProgress);
    }

}
