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

package com.oppo.cloud.parser.domain.job;

import com.oppo.cloud.common.constant.LogType;
import com.oppo.cloud.common.domain.job.LogPath;
import lombok.Data;

import java.util.List;

@Data
public class ParserParam {

    // 日志类型
    private LogType logType;
    // 日志路径
    private List<LogPath> logPaths;
    // APP信息
    private TaskParam taskParam;

    public ParserParam() {

    }

    public ParserParam(LogType logType, List<LogPath> logPaths, TaskParam taskParam) {
        this.logType = logType;
        this.logPaths = logPaths;
        this.taskParam = taskParam;
    }
}
