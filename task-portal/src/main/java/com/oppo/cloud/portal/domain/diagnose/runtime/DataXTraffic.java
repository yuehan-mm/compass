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
@ApiModel("DataX流量异常")
public class DataXTraffic extends IsAbnormal {

    @ApiModelProperty(value = "DataX流量异常表单数据")
    private Table<TaskInfo> table = new Table<>();

    public DataXTraffic() {
        LinkedHashMap<String, String> titleMap = new LinkedHashMap<>();
        titleMap.put("traffic", "流量");
        titleMap.put("maxTraffic", "流量阈值上限");
        titleMap.put("minTraffic", "流量阈值下限");
        table.setTitles(titleMap);
    }

    @Data
    public static class TaskInfo {

        @ApiModelProperty(value = "流量")
        private double traffic;

        @ApiModelProperty(value = "最大阈值")
        private double maxTraffic;

        @ApiModelProperty(value = "最小阈值")
        private double minTraffic;
    }
}
