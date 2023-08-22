package com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality;

import lombok.Data;

@Data
public class DiagnoseContent {
    public DiagnoseResult diagnoseResult;
    public int score;
    public String scoreContent;
}
