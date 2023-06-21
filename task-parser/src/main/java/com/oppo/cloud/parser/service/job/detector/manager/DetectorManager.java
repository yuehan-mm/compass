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

import com.oppo.cloud.common.domain.eventlog.DetectorResult;
import com.oppo.cloud.common.domain.eventlog.DetectorStorage;
import com.oppo.cloud.common.domain.eventlog.config.DetectorConfig;
import com.oppo.cloud.parser.domain.job.DetectorParam;
import com.oppo.cloud.parser.service.job.detector.IDetector;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public abstract class DetectorManager {

    public final DetectorParam param;

    public final DetectorConfig config;

    public DetectorManager(DetectorParam param) {
        this.param = param;
        this.config = param.getConfig();
    }

    public DetectorStorage run() {
        List<IDetector> detectors = this.createDetectors();

        DetectorStorage detectorStorage = new DetectorStorage(
                this.param.getAppType().getValue(),
                this.param.getFlowName(), this.param.getProjectName(),
                this.param.getTaskName(), this.param.getExecutionTime(),
                this.param.getTryNumber(), this.param.getAppId(),
                this.param.getLogPath(), this.config);

        for (IDetector detector : detectors) {
            DetectorResult result;
            try {
                result = detector.detect();
            } catch (Exception e) {
                log.error("Exception:", e);
                continue;
            }
            if (result == null) {
                continue;
            }
            if (result.getAbnormal()) {
                detectorStorage.setAbnormal(result.getAbnormal());
                log.info("DetectorResult:{},{}", this.param.getAppId(), result.getAppCategory());
            }
            detectorStorage.addDetectorResult(result);
        }

        return detectorStorage;
    }

    public boolean durationFilter(Long duration) {
        return this.param.getAppDuration() < duration;
    }

    public abstract List<IDetector> createDetectors();

}
