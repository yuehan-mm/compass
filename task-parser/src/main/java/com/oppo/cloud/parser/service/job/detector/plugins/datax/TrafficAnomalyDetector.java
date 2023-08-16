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

package com.oppo.cloud.parser.service.job.detector.plugins.datax;

import com.alibaba.fastjson2.JSON;
import com.oppo.cloud.common.constant.AppCategoryEnum;
import com.oppo.cloud.common.domain.eventlog.DetectorResult;
import com.oppo.cloud.common.domain.eventlog.TrafficAnomalyAbnormal;
import com.oppo.cloud.common.domain.eventlog.config.TrafficAnomalyDetectorConfig;
import com.oppo.cloud.parser.domain.event.datax.DataXJobRunTimeInfo;
import com.oppo.cloud.parser.domain.job.DetectorParam;
import com.oppo.cloud.parser.service.job.detector.IDetector;
import com.oppo.cloud.parser.utils.ReplayDataXRuntimeLogs;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TrafficAnomalyDetector implements IDetector {

    private final DetectorParam param;

    private final TrafficAnomalyDetectorConfig config;

    private final ReplayDataXRuntimeLogs replayDataXRuntimeLogs;

    public TrafficAnomalyDetector(DetectorParam param) {
        this.param = param;
        this.config = param.getConfig().getTrafficAnomalyDetectorConfig();
        this.replayDataXRuntimeLogs = (ReplayDataXRuntimeLogs) this.param.getReplayEventLogs();
    }

    @Override
    public DetectorResult detect() {
        log.info("start TrafficAnomalyDetector");
        log.info("TrafficAnomalyDetectorConfig : " + JSON.toJSONString(config));
        DetectorResult<TrafficAnomalyAbnormal> detectorResult =
                new DetectorResult<>(AppCategoryEnum.TRAFFIC_ANOMALY.getCategory(), false);

        TrafficAnomalyAbnormal trafficAnomalyAbnormal = new TrafficAnomalyAbnormal();
        DataXJobRunTimeInfo dataXJobRunTimeInfo = replayDataXRuntimeLogs.getDataXJobRunTimeInfo();
        if (dataXJobRunTimeInfo.getSpeedBytes() < config.getMinTraffic()
                || dataXJobRunTimeInfo.getSpeedBytes() > config.getMaxTraffic()) {
            trafficAnomalyAbnormal.setAbnormal(true);
            trafficAnomalyAbnormal.setTraffic(dataXJobRunTimeInfo.getSpeedBytes());
            detectorResult.setData(trafficAnomalyAbnormal);
            detectorResult.setAbnormal(true);
            return detectorResult;
        }
        return null;
    }
}
