package com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality;

import com.oppo.cloud.common.domain.eventlog.FileScanAbnormal;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Date;
import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
public class DiagnoseResult {
    private Date executionDate;
    private String applicationId;
    private int groupByCount;
    private int unionCount;
    private int joinCount;
    private int orderByCount;
    private int sqlLength;
    private Map<String, Integer> refTableMap;
    private FileScanAbnormal.FileScanReport scriptReport;
}
