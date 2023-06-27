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

package com.oppo.cloud.portal.domain.diagnose.info;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@Data
@ApiModel("mapreduceApp运行参数")
public class MapReduceAppInfo extends AppInfo{

    @ApiModelProperty(value = "yarn.app.mapreduce.am.resource.mb")
    private String yarn_app_mapreduce_am_resource_mb;

    @ApiModelProperty(value = "mapreduce.reduce.memory.mb")
    private String mapreduce_reduce_memory_mb;

    @ApiModelProperty(value = "mapreduce.map.memory.mb")
    private String mapreduce_map_memory_mb;

}
