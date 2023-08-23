package com.oppo.cloud.parser.service.job.detector.plugins.spark;

import com.alibaba.fastjson2.JSON;
import com.oppo.cloud.common.constant.AppCategoryEnum;
import com.oppo.cloud.common.domain.eventlog.DetectorResult;
import com.oppo.cloud.common.domain.eventlog.FileScanAbnormal;
import com.oppo.cloud.common.domain.eventlog.config.FileScanConfig;
import com.oppo.cloud.parser.domain.job.ReadFileInfo;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public class FileScanDetector {

    private final FileScanConfig config;

    public FileScanDetector(FileScanConfig config) {
        this.config = config;
    }

    public DetectorResult detect(Collection<ReadFileInfo> readFileInfos) {
        log.info("start FileScanDetector");
        log.info("FileScanDetector : " + JSON.toJSONString(config));
        DetectorResult<FileScanAbnormal> detectorResult = new DetectorResult<>(AppCategoryEnum.FILE_SCAN_ANOMALY.getCategory(), false);
        FileScanAbnormal fileScanAbnormal = new FileScanAbnormal();

        int avgSize = readFileInfos.size() == 0 ? 0 :
                (int) (readFileInfos.stream().map(x -> x.getMaxOffsets()).reduce((x, y) -> x + y).get() / readFileInfos.size());
        if (readFileInfos.size() > config.getMaxFileCount() || avgSize < config.getMinAvgSize()) {
            fileScanAbnormal.setAbnormal(true);
        }
        fileScanAbnormal.setFileCount(readFileInfos.size());
        fileScanAbnormal.setAvgSize(avgSize);
        detectorResult.setData(fileScanAbnormal);
        detectorResult.setAbnormal(fileScanAbnormal.getAbnormal());
        return detectorResult;
    }
}
