package com.oppo.cloud.common.domain.eventlog.config;

import lombok.Data;

@Data
public class SqlScoreConfig {

    private Boolean disable;

    private int minScore;
}
