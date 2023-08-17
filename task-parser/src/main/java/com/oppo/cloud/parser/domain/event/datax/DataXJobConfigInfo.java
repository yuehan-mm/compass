package com.oppo.cloud.parser.domain.event.datax;

import lombok.Data;

@Data
public class DataXJobConfigInfo {
    // 作业配置信息
    private String src;
    private String src_type;
    private String dest;
    private String dest_type;
}
