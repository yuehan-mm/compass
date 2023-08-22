package com.oppo.cloud.common.domain.eventlog.config;

import lombok.Data;

@Data
public class FileScanConfig {

    private Boolean disable;

    private int fileCount;

    private int avgSize;
}
