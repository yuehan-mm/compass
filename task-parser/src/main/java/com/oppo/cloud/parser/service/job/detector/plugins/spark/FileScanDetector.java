package com.oppo.cloud.parser.service.job.detector.plugins.spark;

import com.alibaba.fastjson2.JSON;
import com.oppo.cloud.common.constant.AppCategoryEnum;
import com.oppo.cloud.common.domain.eventlog.DetectorResult;
import com.oppo.cloud.common.domain.eventlog.FileScanAbnormal;
import com.oppo.cloud.common.domain.eventlog.config.FileScanConfig;
import com.oppo.cloud.parser.domain.job.ReadFileInfo;
import com.oppo.cloud.parser.domain.job.SparkExecutorLogParserResult;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public class FileScanDetector {

    private final FileScanConfig config;

    public FileScanDetector(FileScanConfig config) {
        this.config = config;
    }

    public DetectorResult detect(List<SparkExecutorLogParserResult> results) {
        log.info("start FileScanDetector");
        log.info("FileScanDetector : " + JSON.toJSONString(config));
        DetectorResult<FileScanAbnormal> detectorResult = new DetectorResult<>(AppCategoryEnum.FILE_SCAN_ANOMALY.getCategory(), false);
        FileScanAbnormal fileScanAbnormal = new FileScanAbnormal();

        Collection<ReadFileInfo> readFileInfos = parseFileInfo(results);
        int avgSize = (int) (readFileInfos.stream().map(x -> x.getMaxOffsets()).reduce((x, y) -> x + y).get() / readFileInfos.size());
        if (readFileInfos.size() > config.getMaxFileCount() || avgSize < config.getMinAvgSize()) {
            fileScanAbnormal.setAbnormal(true);
            fileScanAbnormal.setFileCount(readFileInfos.size());
            fileScanAbnormal.setAvgSize(avgSize);
            detectorResult.setData(fileScanAbnormal);
            detectorResult.setAbnormal(true);
            return detectorResult;
        }
        return null;
    }


    private Collection<ReadFileInfo> parseFileInfo(List<SparkExecutorLogParserResult> sparkExecutorLogParserResults) {
        Map<String, ReadFileInfo> res = new HashMap<>();
        for (SparkExecutorLogParserResult sparkExecutorLogParserResult : sparkExecutorLogParserResults) {
            Map<String, ReadFileInfo> readFileInfoMap = sparkExecutorLogParserResult.getReadFileInfo();
            readFileInfoMap.forEach((path, readFileInfo) -> {
                if (!res.containsKey(path) || readFileInfo.getMaxOffsets() > res.get(path).getMaxOffsets()) {
                    res.put(path, readFileInfo);
                }
            });
        }
        return res.values();
    }
}
