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
import com.oppo.cloud.common.domain.eventlog.SpeculativeMapReduceAbnormal;
import com.oppo.cloud.common.domain.eventlog.config.DetectorConfig;
import com.oppo.cloud.portal.domain.diagnose.runtime.SpeculativeMapReduce;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * Map/Reduce数量过多
 */
@Service
public class SpeculativeMapReduceService extends RunTimeBaseService<SpeculativeMapReduce> {

    @Override
    public String getCategory() {
        return AppCategoryEnum.SPECULATIVE_MAP_REDUCE.getCategory();
    }

    @Override
    public SpeculativeMapReduce generateData(DetectorResult detectorResult, DetectorConfig config) throws Exception {
        SpeculativeMapReduceAbnormal speculativeMapReduceAbnormal =
                ((JSONObject) detectorResult.getData()).toJavaObject(SpeculativeMapReduceAbnormal.class);


        SpeculativeMapReduce speculativeMapReduce = new SpeculativeMapReduce();
        speculativeMapReduce.setAbnormal(speculativeMapReduceAbnormal.getAbnormal() != null && speculativeMapReduceAbnormal.getAbnormal());
        List<SpeculativeMapReduce.TaskInfo> data = speculativeMapReduce.getTable().getData();

        // map 信息配置
        SpeculativeMapReduce.TaskInfo mapTaskInfo = new SpeculativeMapReduce.TaskInfo();
        mapTaskInfo.setTaskType("Map");
        mapTaskInfo.setCount(speculativeMapReduceAbnormal.getFinishedMaps());
        mapTaskInfo.setThreshold(config.getSpeculativeMapReduceConfig().getMapThreshold());
        data.add(mapTaskInfo);

        // reduce信息配置
        SpeculativeMapReduce.TaskInfo reduceTaskInfo = new SpeculativeMapReduce.TaskInfo();
        reduceTaskInfo.setTaskType("Reduce");
        reduceTaskInfo.setCount(speculativeMapReduceAbnormal.getFinishedReduces());
        reduceTaskInfo.setThreshold(config.getSpeculativeMapReduceConfig().getReduceThreshold());
        data.add(reduceTaskInfo);

        speculativeMapReduce.getVars().put("mapThreshold", String.valueOf(config.getSpeculativeMapReduceConfig().getMapThreshold()));
        speculativeMapReduce.getVars().put("reduceThreshold", String.valueOf(config.getSpeculativeMapReduceConfig().getReduceThreshold()));
        return speculativeMapReduce;
    }

    @Override
    public String generateConclusionDesc(Map<String, String> thresholdMap) {
        return String.format("任务的Map数量建议不要超过%s,任务的Reduce数量建议不要超过%s",
                thresholdMap.getOrDefault("mapThreshold", ""),
                thresholdMap.getOrDefault("reduceThreshold", ""));
    }

    @Override
    public String generateItemDesc() {
        return "Map/Reduce数量过多";
    }

    @Override
    public String getType() {
        return "table";
    }
}
