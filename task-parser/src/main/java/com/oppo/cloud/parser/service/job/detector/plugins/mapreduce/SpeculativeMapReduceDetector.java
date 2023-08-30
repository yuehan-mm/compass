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

package com.oppo.cloud.parser.service.job.detector.plugins.mapreduce;

import com.alibaba.fastjson2.JSON;
import com.oppo.cloud.common.constant.AppCategoryEnum;
import com.oppo.cloud.common.domain.eventlog.DetectorResult;
import com.oppo.cloud.common.domain.eventlog.SpeculativeMapReduceAbnormal;
import com.oppo.cloud.common.domain.eventlog.config.SpeculativeMapReduceConfig;
import com.oppo.cloud.parser.domain.job.DetectorParam;
import com.oppo.cloud.parser.domain.mapreduce.eventlog.JobFinishedEvent;
import com.oppo.cloud.parser.service.job.detector.IDetector;
import com.oppo.cloud.parser.utils.ReplayMapReduceEventLogs;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SpeculativeMapReduceDetector implements IDetector {

    private final DetectorParam param;

    private final SpeculativeMapReduceConfig config;

    private final ReplayMapReduceEventLogs replayMapReduceEventLogs;

    public SpeculativeMapReduceDetector(DetectorParam param) {
        this.param = param;
        this.config = param.getConfig().getSpeculativeMapReduceConfig();
        this.replayMapReduceEventLogs = (ReplayMapReduceEventLogs) this.param.getReplayEventLogs();
    }

    @Override
    public DetectorResult detect() {
        log.debug("start SpeculativeMapReduceDetector");
        log.info("SpeculativeMapReduceConfig : " + JSON.toJSONString(config));
        DetectorResult<SpeculativeMapReduceAbnormal> detectorResult =
                new DetectorResult<>(AppCategoryEnum.SPECULATIVE_MAP_REDUCE.getCategory(), false);

        SpeculativeMapReduceAbnormal speculativeMapReduceAbnormal = new SpeculativeMapReduceAbnormal();
        JobFinishedEvent jobFinishedEvent = replayMapReduceEventLogs.getJobFinishedEvent();
        if (replayMapReduceEventLogs.getMapReduceApplication().getAppDuration() > config.getDuration()
                && (jobFinishedEvent.getFinishedMaps() > config.getMapThreshold()
                || jobFinishedEvent.getFinishedReduces() > config.getReduceThreshold())) {
            speculativeMapReduceAbnormal.setAbnormal(true);
            speculativeMapReduceAbnormal.setFinishedMaps(jobFinishedEvent.getFinishedMaps());
            speculativeMapReduceAbnormal.setFinishedReduces(jobFinishedEvent.getFinishedReduces());
            detectorResult.setData(speculativeMapReduceAbnormal);
            detectorResult.setAbnormal(true);
            return detectorResult;
        }
        return null;
    }
}
