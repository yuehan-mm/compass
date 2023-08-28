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
import com.oppo.cloud.common.domain.eventlog.FileScanAbnormal;
import com.oppo.cloud.common.domain.eventlog.config.DetectorConfig;
import com.oppo.cloud.portal.domain.diagnose.runtime.FileScan;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * 文件扫描异常
 */
@Service
public class FileScanService extends RunTimeBaseService<FileScan> {

    @Override
    public String getCategory() {
        return AppCategoryEnum.FILE_SCAN_ANOMALY.getCategory();
    }

    @Override
    public FileScan generateData(DetectorResult detectorResult, DetectorConfig config) throws Exception {
        FileScanAbnormal fileScanAbnormal = ((JSONObject) detectorResult.getData()).toJavaObject(FileScanAbnormal.class);

        FileScan fileScanTraffic = new FileScan();
        fileScanTraffic.setAbnormal(fileScanAbnormal.getAbnormal() != null && fileScanAbnormal.getAbnormal());

        List<FileScan.TaskInfo> data = fileScanTraffic.getTable().getData();
        FileScan.TaskInfo taskInfo = new FileScan.TaskInfo();
        taskInfo.setFileCount(fileScanAbnormal.getFileCount());
        taskInfo.setMaxFileCount(config.getFileScanConfig().getMaxFileCount());
        taskInfo.setAvgSize(fileScanAbnormal.getAvgSize());
        taskInfo.setMinAvgSize(config.getFileScanConfig().getMinAvgSize());
        data.add(taskInfo);

        fileScanTraffic.getVars().put("fileCount", String.valueOf(fileScanAbnormal.getFileCount()));
        fileScanTraffic.getVars().put("maxFileCount", String.valueOf(config.getFileScanConfig().getMaxFileCount()));
        fileScanTraffic.getVars().put("avgSize", String.valueOf(fileScanAbnormal.getAvgSize()));
        fileScanTraffic.getVars().put("minAvgSize", String.valueOf(config.getFileScanConfig().getMinAvgSize()));

        return fileScanTraffic;
    }

    @Override
    public String generateConclusionDesc(Map<String, String> thresholdMap) {
        return String.format("每次执行扫描的文件数量不要超过%s,平均文件大小不要超过%s",
                thresholdMap.getOrDefault("maxFileCount", "0"),
                thresholdMap.getOrDefault("minAvgSize", "0"));
    }

    @Override
    public String generateItemDesc() {
        return "文件扫描异常";
    }

    @Override
    public String getType() {
        return "table";
    }
}
