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

package com.oppo.cloud.portal.service.diagnose.runtime;

import com.alibaba.fastjson2.JSONObject;
import com.oppo.cloud.common.constant.AppCategoryEnum;
import com.oppo.cloud.common.domain.eventlog.DetectorResult;
import com.oppo.cloud.common.domain.eventlog.TrafficAnomalyAbnormal;
import com.oppo.cloud.common.domain.eventlog.config.DetectorConfig;
import com.oppo.cloud.portal.domain.diagnose.runtime.DataXTraffic;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * DATAX 流量异常
 */
@Service
public class DataXTrafficAnomalyService extends RunTimeBaseService<DataXTraffic> {

    @Override
    public String getCategory() {
        return AppCategoryEnum.TRAFFIC_ANOMALY.getCategory();
    }

    @Override
    public DataXTraffic generateData(DetectorResult detectorResult, DetectorConfig config) throws Exception {
        TrafficAnomalyAbnormal trafficAnomalyAbnormal =
                ((JSONObject) detectorResult.getData()).toJavaObject(TrafficAnomalyAbnormal.class);


        DataXTraffic dataXTraffic = new DataXTraffic();
        dataXTraffic.setAbnormal(trafficAnomalyAbnormal.getAbnormal() != null && trafficAnomalyAbnormal.getAbnormal());

        List<DataXTraffic.TaskInfo> data = dataXTraffic.getTable().getData();
        DataXTraffic.TaskInfo dataXTaskInfo = new DataXTraffic.TaskInfo();
        dataXTaskInfo.setTraffic(trafficAnomalyAbnormal.getTraffic());
        dataXTaskInfo.setMaxTraffic(config.getTrafficAnomalyDetectorConfig().getMaxTraffic());
        dataXTaskInfo.setMinTraffic(config.getTrafficAnomalyDetectorConfig().getMinTraffic());
        data.add(dataXTaskInfo);

        dataXTraffic.getVars().put("minTraffic", String.valueOf(config.getTrafficAnomalyDetectorConfig().getMinTraffic()));
        dataXTraffic.getVars().put("maxTraffic", String.valueOf(config.getTrafficAnomalyDetectorConfig().getMaxTraffic()));
        dataXTraffic.getVars().put("traffic", String.valueOf(trafficAnomalyAbnormal.getTraffic()));

        return dataXTraffic;
    }

    @Override
    public String generateConclusionDesc(Map<String, String> thresholdMap) {
        return String.format("一次抽取数据的流量不能小于%s,不要超过%s",
                thresholdMap.getOrDefault("minTraffic", "0"),
                thresholdMap.getOrDefault("maxTraffic", "999999"));
    }

    @Override
    public String generateItemDesc() {
        return "DataX流量异常";
    }

    @Override
    public String getType() {
        return "table";
    }
}
