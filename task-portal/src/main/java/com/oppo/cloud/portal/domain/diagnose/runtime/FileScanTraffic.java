/*
 * Copyright 2023 OPPO.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.oppo.cloud.portal.domain.diagnose.runtime;

import com.oppo.cloud.portal.domain.diagnose.IsAbnormal;
import com.oppo.cloud.portal.domain.diagnose.Table;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import java.util.LinkedHashMap;


@Data
@ApiModel("文件扫描异常")
public class FileScanTraffic extends IsAbnormal {

    @ApiModelProperty(value = "文件扫描异常")
    private Table<TaskInfo> table = new Table<>();

    public FileScanTraffic() {
        LinkedHashMap<String, String> titleMap = new LinkedHashMap<>();
        titleMap.put("fileCount", "文件数量");
        titleMap.put("MaxFileCount", "文件数量阈值");
        titleMap.put("avgSize", "平均文件大小");
        titleMap.put("MaxAvgSize", "平均文件大小阈值");
        table.setTitles(titleMap);
    }

    @Data
    public static class TaskInfo {

        @ApiModelProperty(value = "文件数量")
        private double fileCount;

        @ApiModelProperty(value = "文件数量阈值")
        private double maxFileCount;

        @ApiModelProperty(value = "平均文件大小")
        private double avgSize;

        @ApiModelProperty(value = "平均文件大小阈值")
        private double maxAvgSize;
    }
}
