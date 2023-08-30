package com.oppo.cloud.parser.service.job.detector.plugins.spark;

import com.alibaba.fastjson2.JSON;
import com.oppo.cloud.common.constant.AppCategoryEnum;
import com.oppo.cloud.common.domain.eventlog.DetectorResult;
import com.oppo.cloud.common.domain.eventlog.FileScanAbnormal;
import com.oppo.cloud.common.domain.eventlog.config.FileScanConfig;
import com.oppo.cloud.parser.domain.job.ReadFileInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

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
        FileScanAbnormal fileScanAbnormal = buildFileScanAbnormal(readFileInfos);
        detectorResult.setData(fileScanAbnormal);
        detectorResult.setAbnormal(fileScanAbnormal.getAbnormal());
        return detectorResult;
    }

    /**
     * 异常描述信息构建
     *
     * @param readFileInfos
     * @return
     */
    private FileScanAbnormal buildFileScanAbnormal(Collection<ReadFileInfo> readFileInfos) {
        FileScanAbnormal fileScanAbnormal = new FileScanAbnormal();
        FileScanAbnormal.FileScanReport scriptReport = buildFileScanReport("ALL", readFileInfos);
        fileScanAbnormal.setScriptReport(scriptReport);
        fileScanAbnormal.setAbnormal(scriptReport.getTotalFileCount() > config.getMaxFileCount() || scriptReport.getFileSizeAvg() < config.getMinAvgSize());
        fileScanAbnormal.setTableReport(buildTableFileScanReport(readFileInfos));
        return fileScanAbnormal;
    }

    /**
     * 按表分类
     *
     * @param readFileInfos
     * @return
     */
    private List<FileScanAbnormal.FileScanReport> buildTableFileScanReport(Collection<ReadFileInfo> readFileInfos) {
        List<FileScanAbnormal.FileScanReport> tableReport = new ArrayList<>();
        readFileInfos
                .stream()
                .collect(Collectors.groupingBy(ReadFileInfo::getTableName))
                .forEach((tableName, fileList) -> {
                    tableReport.add(buildFileScanReport(tableName, fileList));
                });
        return tableReport;
    }

    /**
     * 获取文件扫描结果信息
     *
     * @param tableName
     * @param fileList
     * @return
     */
    private FileScanAbnormal.FileScanReport buildFileScanReport(String tableName, Collection<ReadFileInfo> fileList) {
        int totalFileCount = fileList.size();
        long totalFileSize = fileList.stream().map(x -> x.getMaxOffsets()).reduce((x, y) -> x + y).orElse(0l);
        int fileSizeAvg = fileList.size() == 0 ? 0 :
                (int) (fileList.stream().map(x -> x.getMaxOffsets()).reduce((x, y) -> x + y).get() / fileList.size());
        int le10MFileCount = (int) fileList.stream().filter(x -> x.getMaxOffsets() < 10 * 1024 * 1024).count();
        int partitionCount = (int) fileList.stream().map(x -> x.getPartitionName()).filter(x -> StringUtils.isNotEmpty(x)).distinct().count();
        long partitionSizeAvg = partitionCount == 0 ? 0 : totalFileSize / partitionCount;
        return new FileScanAbnormal.FileScanReport(tableName, totalFileCount, totalFileSize, fileSizeAvg, le10MFileCount, partitionCount, partitionSizeAvg);
    }
}
