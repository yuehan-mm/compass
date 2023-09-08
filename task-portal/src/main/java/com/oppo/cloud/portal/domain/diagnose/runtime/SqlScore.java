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
@ApiModel("SQL评分过低")
public class SqlScore extends IsAbnormal {

    @ApiModelProperty(value = "SQL评分过低")
    private Table<TaskInfo> table = new Table<>();

    public SqlScore() {
        LinkedHashMap<String, String> titleMap = new LinkedHashMap<>();
        titleMap.put("sqlScore", "SQL评分");
        titleMap.put("minScore", "评分阈值");
        titleMap.put("sqlScoreContent", "评分明细");
        table.setTitles(titleMap);
    }

    @Data
    public static class TaskInfo {
        @ApiModelProperty(value = "SQL评分")
        private double sqlScore;

        @ApiModelProperty(value = "评分阈值")
        private int minScore;

        @ApiModelProperty(value = "评分明细")
        private String sqlScoreContent;
    }
}
