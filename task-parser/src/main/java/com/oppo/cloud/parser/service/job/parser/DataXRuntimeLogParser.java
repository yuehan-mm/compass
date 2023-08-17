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
import com.oppo.cloud.common.domain.job.LogPath;
import com.oppo.cloud.common.domain.oneclick.OneClickProgress;
import com.oppo.cloud.common.domain.oneclick.ProgressInfo;
import com.oppo.cloud.common.util.spring.SpringBeanUtil;
import com.oppo.cloud.parser.domain.job.CommonResult;
import com.oppo.cloud.parser.domain.job.DetectorParam;
import com.oppo.cloud.parser.domain.job.ParserParam;
import com.oppo.cloud.parser.domain.reader.ReaderObject;
import com.oppo.cloud.parser.service.job.detector.manager.DataXDetectorManager;
import com.oppo.cloud.parser.service.job.detector.manager.DetectorManager;
import com.oppo.cloud.parser.service.job.oneclick.OneClickSubject;
import com.oppo.cloud.parser.service.reader.IReader;
import com.oppo.cloud.parser.service.reader.LogReaderFactory;
import com.oppo.cloud.parser.service.rules.JobRulesConfigService;
import com.oppo.cloud.parser.utils.ReplayDataXRuntimeLogs;
import com.oppo.cloud.parser.utils.ReplayEventLogs;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class DataXRuntimeLogParser extends OneClickSubject implements IParser {

    private final ParserParam param;

    private DetectorConfig config;

    private boolean isOneClick;

    public DataXRuntimeLogParser(ParserParam param) {
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
            List<ReaderObject> readerObjects;
            try {
                IReader reader = LogReaderFactory.create(logPath);
                readerObjects = reader.getReaderObjectsByFuzzyPath();
            } catch (Exception e) {
                log.error("Exception:", e);
                updateParserProgress(ProgressState.FAILED, 0, 0);
                return null;
            }
            return parse(readerObjects);
        }
        return null;
    }

    private CommonResult<DetectorStorage> parse(List<ReaderObject> readerObjects) {
        ReplayDataXRuntimeLogs replayDataXRuntimeLogs = new ReplayDataXRuntimeLogs(
                this.param.getTaskParam().getTaskApp().getApplicationType());
        try {
            for (ReaderObject readerObject : readerObjects) {
                replayDataXRuntimeLogs.replay(readerObject);
            }
        } catch (Exception e) {
            log.error("replay dataX runtime log error.", e);
            updateParserProgress(ProgressState.FAILED, 0, 0);
            return null;
        }
        log.info("replay result: " + JSON.toJSONString(replayDataXRuntimeLogs));
        return detect(replayDataXRuntimeLogs, null);
    }

    private CommonResult<DetectorStorage> detect(ReplayDataXRuntimeLogs replayEventLogs, String logPath) {
        Map<String, Object> env = getDataXEnvironmentConfig(replayEventLogs);

        DetectorParam detectorParam = new DetectorParam(this.param.getTaskParam().getTaskApp().getFlowName(),
                this.param.getTaskParam().getTaskApp().getProjectName(),
                this.param.getTaskParam().getTaskApp().getTaskName(),
                this.param.getTaskParam().getTaskApp().getExecutionDate(),
                this.param.getTaskParam().getTaskApp().getRetryTimes(),
                this.param.getTaskParam().getTaskApp().getApplicationId(),
                this.param.getTaskParam().getTaskApp().getApplicationType(),
                replayEventLogs.getDataXJobRunTimeInfo().getAppDuration(),
                logPath, config, replayEventLogs, isOneClick);

        DetectorManager detectorManager = new DataXDetectorManager(detectorParam);
        // run all detector
        DetectorStorage detectorStorage = detectorManager.run();

        detectorStorage.setEnv(env);

        CommonResult<DetectorStorage> result = new CommonResult<>();
        result.setLogType(this.param.getLogType());
        result.setResult(detectorStorage);

        updateParserProgress(ProgressState.SUCCEED, 0, 0);
        return result;
    }

    private Map<String, Object> getDataXEnvironmentConfig(ReplayDataXRuntimeLogs replayEventLogs) {
        Map<String, Object> env = new HashMap<>();
        env.put("src", replayEventLogs.getDataXJobConfigInfo().getSrcTable());
        env.put("src_type", replayEventLogs.getDataXJobConfigInfo().getSrcTable());
        env.put("dest", replayEventLogs.getDataXJobConfigInfo().getDestTable());
        env.put("dest_type", replayEventLogs.getDataXJobConfigInfo().getDestTable());
        return env;
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
