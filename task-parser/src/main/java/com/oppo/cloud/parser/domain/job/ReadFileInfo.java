package com.oppo.cloud.parser.domain.job;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ReadFileInfo {
    private String filePath;
    private long maxOffsets;
    private String tableName;
    private String partitionName;
    private String readType;
}
