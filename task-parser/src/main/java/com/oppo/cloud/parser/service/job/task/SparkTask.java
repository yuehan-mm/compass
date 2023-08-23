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
import com.oppo.cloud.common.domain.eventlog.config.DetectorConfig;
import com.oppo.cloud.common.domain.gc.GCReport;
import com.oppo.cloud.common.util.spring.SpringBeanUtil;
import com.oppo.cloud.parser.config.ThreadPoolConfig;
import com.oppo.cloud.parser.domain.job.*;
import com.oppo.cloud.parser.service.job.detector.plugins.spark.FileScanDetector;
import com.oppo.cloud.parser.service.job.detector.plugins.spark.MemWasteDetector;
import com.oppo.cloud.parser.service.job.detector.plugins.spark.SqlScoreDetector;
import com.oppo.cloud.parser.service.job.parser.IParser;
import com.oppo.cloud.parser.service.rules.JobRulesConfigService;
import com.oppo.cloud.parser.service.writer.ElasticWriter;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

/**
 * Spark task
 */
@Slf4j
public class SparkTask extends Task {

    private final TaskParam taskParam;

    private final Executor taskThreadPool;

    private final DetectorConfig detectorConfig;

    public SparkTask(TaskParam taskParam) {
        super(taskParam);
        this.taskParam = taskParam;
        this.taskThreadPool = (ThreadPoolTaskExecutor) SpringBeanUtil.getBean(ThreadPoolConfig.TASK_THREAD_POOL);
        this.detectorConfig = ((JobRulesConfigService) SpringBeanUtil.getBean(JobRulesConfigService.class)).detectorConfig;
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

        List<SparkExecutorLogParserResult> sparkExecutorLogParserResults = null;
        SparkEventLogParserResult sparkEventLogParserResult = null;

        for (Future<CommonResult> result : futures) {
            CommonResult commonResult;
            try {
                commonResult = result.get();
                if (commonResult != null) {
                    switch (commonResult.getLogType()) {
                        case SPARK_DRIVER:
                        case SPARK_EXECUTOR:
                            sparkExecutorLogParserResults =
                                    (List<SparkExecutorLogParserResult>) commonResult.getResult();
                            break;
                        case SPARK_EVENT:
                            sparkEventLogParserResult = (SparkEventLogParserResult) commonResult.getResult();
                            break;
                        default:
                            break;
                    }
                }
            } catch (Exception e) {
                log.error("Exception:{}", e);
            }
        }

        if (sparkEventLogParserResult == null) {
            log.error("sparkEventLogParserResultNull:{}", taskParam);
            return null;
        }

        // extract executor info
        ExecutorLogInfo executorLogInfo = this.extractExecutorLogParserResults(sparkExecutorLogParserResults);
        log.error("000--"+JSON.toJSONString(executorLogInfo));
        DetectorStorage detectorStorage = sparkEventLogParserResult.getDetectorStorage();

        if (detectorStorage == null) {
            log.error("detectorStorageNull:{}", taskParam);
            taskResult.setCategories(executorLogInfo.executorCategories);
            return taskResult;
        }

        // get executor log abnormal
        List<DetectorResult> detectorResultList = this.getExecutorLogAbnormal(executorLogInfo, sparkEventLogParserResult.getMemoryCalculateParam());

        log.error("1111--"+JSON.toJSONString(detectorResultList));
        log.error("222--"+JSON.toJSONString(executorLogInfo.executorCategories));
        detectorStorage.addDetectorResult(detectorResultList);
        detectorStorage.setAbnormal(detectorStorage.getDataList().stream().map(x -> x.getAbnormal()).reduce((x, y) -> x && y).get());

        // save and return all spark categories
        List<String> eventLogCategories = this.saveAndReturnCategories(detectorStorage, executorLogInfo.executorCategories);
        log.error("333--"+JSON.toJSONString(eventLogCategories));
        taskResult.setCategories(eventLogCategories);

        // save gc reports
        this.saveGcReport(executorLogInfo.gcReports, eventLogCategories, detectorStorage);

        return taskResult;
    }

    private void saveGcReport(List<GCReport> gcReports, List<String> eventLogCategories, DetectorStorage detectorStorage) {
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
    }

    private List<String> saveAndReturnCategories(DetectorStorage detectorStorage, List<String> executorCategories) {
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
        eventLogCategories.addAll(executorCategories);
        return eventLogCategories;
    }

    /**
     * 进行 executor 相关异常诊断。
     * 包含：文件扫描，SQL评分，内存浪费
     *
     * @param executorLogInfo
     * @param memoryCalculateParam
     * @return
     */
    private List<DetectorResult> getExecutorLogAbnormal(ExecutorLogInfo executorLogInfo, MemoryCalculateParam memoryCalculateParam) {
        List<DetectorResult> detectorResultList = new ArrayList<>();

        // fileScan abnormal
        if (!this.detectorConfig.getFileScanConfig().getDisable()) {
            FileScanDetector fileScanDetector = new FileScanDetector(this.detectorConfig.getFileScanConfig());
            detectorResultList.add(fileScanDetector.detect(executorLogInfo.readFileInfos));
        }

        // sqlScore abnormal
        if (!this.detectorConfig.getSqlScoreConfig().getDisable() && StringUtils.isNotEmpty(executorLogInfo.sqlCommand)) {
            SqlScoreDetector sqlScoreDetector = new SqlScoreDetector(this.detectorConfig.getSqlScoreConfig());
            detectorResultList.add(sqlScoreDetector.detect(executorLogInfo.sqlCommand, taskParam.getTaskApp().getTaskName()));
        }

        // mem waste abnormal
        if (!this.detectorConfig.getMemWasteConfig().getDisable() && executorLogInfo.gcReports.size() > 0 && memoryCalculateParam != null) {
            MemWasteDetector memWasteDetector = new MemWasteDetector(this.detectorConfig.getMemWasteConfig());
            detectorResultList.add(memWasteDetector.detect(executorLogInfo.gcReports, memoryCalculateParam));
        }

        return detectorResultList;
    }

    /**
     * 提取 executor log 解析结果
     * 包含：gcReport，executorCategories，readFileInfo，sqlCommand
     *
     * @param sparkExecutorLogParserResults
     * @return
     */
    private ExecutorLogInfo extractExecutorLogParserResults(List<SparkExecutorLogParserResult> sparkExecutorLogParserResults) {
        if (sparkExecutorLogParserResults == null) return new ExecutorLogInfo();

        List<String> executorCategories = new ArrayList<>();
        List<GCReport> gcReports = new ArrayList<>();
        Map<String, ReadFileInfo> readFileInfos = new HashMap<>();
        StringBuffer sqlCommand = new StringBuffer();

        for (SparkExecutorLogParserResult result : sparkExecutorLogParserResults) {
            if (result.getGcReports() != null) {
                gcReports.addAll(result.getGcReports());
            }
            if (result.getCategories() != null) {
                executorCategories.addAll(result.getCategories());
            }
            if (result.getReadFileInfo() != null) {
                result.getReadFileInfo().forEach((path, readFileInfo) -> {
                    if (!readFileInfos.containsKey(path)
                            || readFileInfo.getMaxOffsets() > readFileInfos.get(path).getMaxOffsets()) {
                        readFileInfos.put(path, readFileInfo);
                    }
                });
            }
            if (StringUtils.isNotEmpty(result.getSqlCommand())) {
                sqlCommand.append(result.getSqlCommand() + "\n");
            }
        }
        return new ExecutorLogInfo(executorCategories, gcReports, readFileInfos.values(), sqlCommand.toString());
    }


    @AllArgsConstructor
    private class ExecutorLogInfo {
        private List<String> executorCategories;
        private List<GCReport> gcReports;
        private Collection<ReadFileInfo> readFileInfos;
        private String sqlCommand;

        public ExecutorLogInfo() {
            this.executorCategories = new ArrayList<>();
            this.gcReports = new ArrayList<>();
            this.readFileInfos = new ArrayList<>();
            this.sqlCommand = null;
        }
    }
}
