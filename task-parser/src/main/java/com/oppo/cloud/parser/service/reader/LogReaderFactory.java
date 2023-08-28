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

package com.oppo.cloud.parser.service.reader;

import com.oppo.cloud.common.domain.job.LogPath;

/**
 * 日志读取工厂
 */
public class LogReaderFactory {

    private static final String HDFS = "hdfs";
    private static final String S3 = "s3";
    private static final String OSS = "oss";
    /**
     * 创建读取类型
     */
    public static IReader create(LogPath logPath) throws Exception {
        switch (logPath.getProtocol()) {
            case HDFS:
            case OSS:
                return new IReader(logPath);
            default:
                break;
        }
        return new IReader(logPath);
    }
}
