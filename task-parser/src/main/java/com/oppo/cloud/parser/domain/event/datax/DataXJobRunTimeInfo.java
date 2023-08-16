package com.oppo.cloud.parser.domain.event.datax;

import lombok.Data;

/**
 * 作业运行时信息
 */
@Data
public class DataXJobRunTimeInfo {
    // 流量
    private double totalRecords;
    private double totalBytes;
    private double speedBytes;
    private double speedRows;
    private double errorBytes;
    private double errorRows;
    private double allTaskWaitWriterTime;
    private double allTaskWaitReadTime;
    private double percentage;

    // 运行时间
    private Long appDuration;
    private Long appStartTimestamp;
    private Long appEndTimestamp;
}
