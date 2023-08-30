package com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality;

import com.oppo.cloud.common.domain.eventlog.FileScanAbnormal;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
public class DiagnoseResult {
    private int groupByCount;
    private int unionCount;
    private int joinCount;
    private int orderByCount;
    private int sqlLength;
    private List<String> insertList;
    private List<String> memConfList;
    private Map<String, Integer> refTableMap;
    private FileScanAbnormal.FileScanReport scriptReport;
    private List<FileScanAbnormal.FileScanReport> tableReport;
}
