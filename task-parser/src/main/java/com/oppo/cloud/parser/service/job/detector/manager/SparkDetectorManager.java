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

package com.oppo.cloud.parser.service.job.detector.manager;

import com.oppo.cloud.common.domain.eventlog.config.*;
import com.oppo.cloud.parser.domain.job.DetectorParam;
import com.oppo.cloud.parser.service.job.detector.*;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class SparkDetectorManager extends DetectorManager {

    public SparkDetectorManager(DetectorParam param) {
        super(param);
    }

    @Override
    public List<IDetector> createDetectors() {
        if (this.param.isOneClick()) {
            return createOneClickDetectors();
        } else {
            return createAllDetectors();
        }
    }

    private List<IDetector> createOneClickDetectors() {
        List<IDetector> detectors = new ArrayList<>();
        detectors.add(new CpuWasteDetector(param));
        detectors.add(new DataSkewDetector(param));
        detectors.add(new GlobalSortDetector(param));
        detectors.add(new HdfsStuckDetector(param));
        detectors.add(new JobDurationDetector(param));
        detectors.add(new LargeTableScanDetector(param));
        detectors.add(new OOMWarnDetector(param));
        detectors.add(new SpeculativeTaskDetector(param));
        detectors.add(new StageDurationDetector(param));
        detectors.add(new TaskDurationDetector(param));
        return detectors;
    }

    public List<IDetector> createAllDetectors() {
        List<IDetector> detectors = new ArrayList<>();

        CpuWasteConfig cpuWasteConfig = this.config.getCpuWasteConfig();
        if (!cpuWasteConfig.getDisable() && !durationFilter(cpuWasteConfig.getDuration())) {
            detectors.add(new CpuWasteDetector(param));
        }

        DataSkewConfig dataSkewConfig = this.config.getDataSkewConfig();
        if (!dataSkewConfig.getDisable() && !durationFilter(dataSkewConfig.getDuration())) {
            detectors.add(new DataSkewDetector(param));
        }

        GlobalSortConfig globalSortConfig = this.config.getGlobalSortConfig();
        if (!globalSortConfig.getDisable()) {
            detectors.add(new GlobalSortDetector(param));
        }

        HdfsStuckConfig hdfsStuckConfig = this.config.getHdfsStuckConfig();
        if (!hdfsStuckConfig.getDisable()) {
            detectors.add(new HdfsStuckDetector(param));
        }

        JobDurationConfig jobDurationConfig = this.config.getJobDurationConfig();
        if (!jobDurationConfig.getDisable()) {
            detectors.add(new JobDurationDetector(param));
        }

        LargeTableScanConfig largeTableScanConfig = this.config.getLargeTableScanConfig();
        if (!largeTableScanConfig.getDisable() && !durationFilter(largeTableScanConfig.getDuration())) {
            detectors.add(new LargeTableScanDetector(param));
        }

        OOMWarnConfig oomWarnConfig = this.config.getOomWarnConfig();
        if (!oomWarnConfig.getDisable() && !durationFilter(oomWarnConfig.getDuration())) {
            detectors.add(new OOMWarnDetector(param));
        }

        SpeculativeTaskConfig speculativeTaskConfig = this.config.getSpeculativeTaskConfig();
        if (!speculativeTaskConfig.getDisable() && !durationFilter(speculativeTaskConfig.getDuration())) {
            detectors.add(new SpeculativeTaskDetector(param));
        }

        StageDurationConfig stageDurationConfig = this.config.getStageDurationConfig();
        if (!stageDurationConfig.getDisable()) {
            detectors.add(new StageDurationDetector(param));
        }

        TaskDurationConfig taskDurationConfig = this.config.getTaskDurationConfig();
        if (!this.config.getTaskDurationConfig().getDisable() && !durationFilter(taskDurationConfig.getDuration())) {
            detectors.add(new TaskDurationDetector(param));
        }

        return detectors;
    }

}
