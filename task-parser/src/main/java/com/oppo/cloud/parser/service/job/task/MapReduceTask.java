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

package com.oppo.cloud.parser.service.job.task;

import com.alibaba.fastjson2.JSON;
import com.oppo.cloud.common.domain.eventlog.DetectorResult;
import com.oppo.cloud.common.domain.eventlog.DetectorStorage;
import com.oppo.cloud.common.domain.eventlog.config.MemWasteConfig;
import com.oppo.cloud.common.domain.gc.GCReport;
import com.oppo.cloud.common.util.spring.SpringBeanUtil;
import com.oppo.cloud.parser.config.ThreadPoolConfig;
import com.oppo.cloud.parser.domain.job.*;
import com.oppo.cloud.parser.service.job.detector.MemWasteDetector;
import com.oppo.cloud.parser.service.job.parser.IParser;
import com.oppo.cloud.parser.service.rules.JobRulesConfigService;
import com.oppo.cloud.parser.service.writer.ElasticWriter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

/**
 * Spark task
 */
@Slf4j
public class MapReduceTask extends Task {

    private final TaskParam taskParam;
    private final MemWasteConfig memWasteConfig;
    private final Executor taskThreadPool;

    public MapReduceTask(TaskParam taskParam) {
        super(taskParam);
        this.taskParam = taskParam;
        taskThreadPool = (ThreadPoolTaskExecutor) SpringBeanUtil.getBean(ThreadPoolConfig.TASK_THREAD_POOL);
        JobRulesConfigService jobRulesConfigService = (JobRulesConfigService) SpringBeanUtil.getBean(JobRulesConfigService.class);
        this.memWasteConfig = jobRulesConfigService.detectorConfig.getMemWasteConfig();
    }

    @Override
    public TaskResult run() {
        List<IParser> parsers = super.createTasks();
        if (parsers.size() == 0) {
            return null;
        }
        List<CompletableFuture<CommonResult>> futures = super.createFutures(parsers, taskThreadPool);

        TaskResult taskResult = new TaskResult();
        taskResult.setAppId(this.taskParam.getTaskApp().getApplicationId());

        List<GCReport> gcReports = new ArrayList<>();
        List<MapReduceTaskManagerLogParserResult> mapReduceTaskManagerLogParserResults = null;
        MapReduceEventLogParserResult mapReduceEventLogParserResult = null;

        for (Future<CommonResult> result : futures) {
            CommonResult commonResult;
            try {
                commonResult = result.get();
                if (commonResult != null) {
                    switch (commonResult.getLogType()) {
                        case CONTAINER:
                            mapReduceTaskManagerLogParserResults =
                                    (List<MapReduceTaskManagerLogParserResult>) commonResult.getResult();
                            break;
                        case MAPREDUCE_EVENT:
                            mapReduceEventLogParserResult = (MapReduceEventLogParserResult) commonResult.getResult();
                            break;
                        default:
                            break;
                    }
                }
            } catch (Exception e) {
                log.error("Exception:{}", e);
            }
        }

        if (mapReduceEventLogParserResult == null) {
            log.error("mapReduceEventLogParserResultNull:{}", taskParam);
            return null;
        }
        // get driver/executor categories
        if (mapReduceTaskManagerLogParserResults != null) {
            for (MapReduceTaskManagerLogParserResult result : mapReduceTaskManagerLogParserResults) {
                if (result.getGcReports() != null) {
                    gcReports.addAll(result.getGcReports());
                }
            }
        }

        DetectorStorage detectorStorage = mapReduceEventLogParserResult.getDetectorStorage();
        log.info("MR DetectorStorage : " + JSON.toJSONString(detectorStorage));
        if (detectorStorage == null) {
            log.error("detectorStorageNull:{}", taskParam);
            return taskResult;
        }
        // calculate memory metrics
        if (!this.memWasteConfig.getDisable() && gcReports.size() > 0
                && mapReduceEventLogParserResult.getMemoryCalculateParam() != null) {
            MemWasteDetector memWasteDetector = new MemWasteDetector(this.memWasteConfig);
            DetectorResult detectorResult =
                    memWasteDetector.detect(gcReports, mapReduceEventLogParserResult.getMemoryCalculateParam());
            detectorStorage.addDetectorResult(detectorResult);
            if (detectorResult.getAbnormal()) {
                detectorStorage.setAbnormal(true);
            }
        }
        // get event log categories
        List<String> eventLogCategories = new ArrayList<>();
        if (this.taskParam.getIsOneClick() || detectorStorage.getAbnormal()) {
            for (DetectorResult detectorResult : detectorStorage.getDataList()) {
                if (detectorResult.getAbnormal()) {
                    eventLogCategories.add(detectorResult.getAppCategory());
                }
            }
            // save all detector results
            ElasticWriter.getInstance().saveDetectorStorage(detectorStorage);
        } else {
            // save event log env
            detectorStorage.setDataList(new ArrayList<>());
            ElasticWriter.getInstance().saveDetectorStorage(detectorStorage);
        }

        // set all spark categories
        taskResult.setCategories(eventLogCategories);

        // save gc reports
        if ((this.taskParam.getIsOneClick() || eventLogCategories.size() > 0) && gcReports.size() > 0) {
            gcReports.sort(Comparator.comparing(GCReport::getMaxHeapUsedSize));
            if (gcReports.size() > 11) {
                List<GCReport> results = new ArrayList<>();
                GCReport driverGc = gcReports.stream().filter(gc -> gc.getExecutorId() == 0).findFirst().orElse(null);
                List<GCReport> executorGcs = gcReports.subList(gcReports.size() - 10, gcReports.size());
                if (driverGc != null) {
                    results.add(driverGc);
                }
                results.addAll(executorGcs);
                ElasticWriter.getInstance().saveGCReports(results, detectorStorage.getExecutionTime(),
                        detectorStorage.getApplicationId());
            } else {
                ElasticWriter.getInstance().saveGCReports(gcReports, detectorStorage.getExecutionTime(),
                        detectorStorage.getApplicationId());
            }
        }

        return taskResult;
    }
}
