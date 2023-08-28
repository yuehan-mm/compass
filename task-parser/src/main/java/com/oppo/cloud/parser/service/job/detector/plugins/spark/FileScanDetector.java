package com.oppo.cloud.parser.service.job.detector.plugins.spark;

import com.alibaba.fastjson2.JSON;
import com.oppo.cloud.common.constant.AppCategoryEnum;
import com.oppo.cloud.common.domain.eventlog.DetectorResult;
import com.oppo.cloud.common.domain.eventlog.FileScanAbnormal;
import com.oppo.cloud.common.domain.eventlog.config.FileScanConfig;
import com.oppo.cloud.parser.domain.job.ReadFileInfo;
import com.oppo.cloud.parser.domain.job.TaskParam;
import com.oppo.cloud.parser.service.writer.MysqlWriter;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class FileScanDetector {

    private final FileScanConfig config;

    public FileScanDetector(FileScanConfig config) {
        this.config = config;
    }

    public DetectorResult detect(Collection<ReadFileInfo> readFileInfos, TaskParam taskParam) {
        // TODO 临时更新文件报告
        updateFileScanReport(readFileInfos, taskParam);

        log.info("start FileScanDetector");
        log.info("FileScanDetector : " + JSON.toJSONString(config));
        DetectorResult<FileScanAbnormal> detectorResult = new DetectorResult<>(AppCategoryEnum.FILE_SCAN_ANOMALY.getCategory(), false);
        FileScanAbnormal fileScanAbnormal = new FileScanAbnormal();

        int avgSize = readFileInfos.size() == 0 ? 0 :
                (int) (readFileInfos.stream().map(x -> x.getMaxOffsets()).reduce((x, y) -> x + y).get() / readFileInfos.size());

        boolean isAbnormal = readFileInfos.size() > config.getMaxFileCount() || avgSize < config.getMinAvgSize();

        fileScanAbnormal.setAbnormal(isAbnormal);
        fileScanAbnormal.setFileCount(readFileInfos.size());
        fileScanAbnormal.setAvgSize(avgSize);
        detectorResult.setData(fileScanAbnormal);
        detectorResult.setAbnormal(isAbnormal);
        return detectorResult;
    }

    private void updateFileScanReport(Collection<ReadFileInfo> readFileInfos, TaskParam taskParam) {
        List<FileScanReport> list = new ArrayList<>();
        list.add(buildFileScanReport("ALL", readFileInfos));
        readFileInfos
                .stream()
                .collect(Collectors.groupingBy(ReadFileInfo::getTableName))
                .forEach((tableName, fileList) -> {
                    list.add(buildFileScanReport(tableName, fileList));
                });
        MysqlWriter.getInstance().updateOffLineData2(JSON.toJSONString(list), taskParam);
    }

    private FileScanReport buildFileScanReport(String tableName, Collection<ReadFileInfo> fileList) {
        int totalFileCount = fileList.size();
        long totalFileSize = fileList.stream().map(x -> x.getMaxOffsets()).reduce((x, y) -> x + y).get();
        int fileSizeAvg = fileList.size() == 0 ? 0 :
                (int) (fileList.stream().map(x -> x.getMaxOffsets()).reduce((x, y) -> x + y).get() / fileList.size());
        int le10MFileCount = (int) fileList.stream().filter(x -> x.getMaxOffsets() < 10 * 1024 * 1024).count();
        int partitionCount = (int) fileList.stream().map(x -> x.getPartitionName()).distinct().count();
        long partitionSizeAvg = totalFileSize / partitionCount;
        return new FileScanReport(tableName, totalFileCount, totalFileSize, fileSizeAvg, le10MFileCount, partitionCount, partitionSizeAvg);
    }

    @Data
    @AllArgsConstructor
    class FileScanReport {
        private String tableName;
        private int totalFileCount;
        private long totalFileSize;
        private int fileSizeAvg;
        private int le10MFileCount;
        private int partitionCount;
        private long partitionSizeAvg;
    }
}
