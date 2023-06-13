package com.oppo.cloud.parser.domain.mapreduce.jobhistory;

import lombok.Data;

@Data
public class JobFinishedEvent {
    private String jobid;
    private long finishTime;
    private int finishedMaps;
    private int finishedReduces;
    private int failedMaps;
    private int failedReduces;
}
