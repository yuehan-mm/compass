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

import com.oppo.cloud.common.domain.cluster.hadoop.NameNodeConf;
import com.oppo.cloud.common.domain.job.LogPath;
import com.oppo.cloud.common.util.spring.SpringBeanUtil;
import com.oppo.cloud.parser.config.HadoopConfig;
import com.oppo.cloud.parser.domain.reader.ReaderObject;
import com.oppo.cloud.parser.utils.HDFSUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 读取OSS类型日志
 */
public class OSSReader implements IReader {

    /**
     * 路径
     */
    private final LogPath logPath;
    /**
     * nameNode配置
     */
    private final NameNodeConf nameNode;

    private final String FILE_SYSTEM_TYPE = "OSS";

    public OSSReader(LogPath logPath) throws Exception {
        this.logPath = logPath;
        Map<String, NameNodeConf> nameNodeMap =
                (Map<String, NameNodeConf>) SpringBeanUtil.getBean(HadoopConfig.NAME_NODE_MAP);
        nameNode = HDFSUtil.getNameNode(nameNodeMap, logPath.getLogPath());
        if (nameNode == null) {
            throw new Exception("cant get hdfs nameNode" + logPath.getLogPath());
        }
    }


    @Override
    public List<String> listFiles() throws Exception {
        return HDFSUtil.listFiles(nameNode, logPath.getLogPath(), FILE_SYSTEM_TYPE);
    }

    @Override
    public ReaderObject getReaderObject() throws Exception {
        return HDFSUtil.getReaderObject(nameNode, logPath.getLogPath(), FILE_SYSTEM_TYPE);
    }

    @Override
    public List<ReaderObject> getReaderObjects() throws Exception {
        List<ReaderObject> list = new ArrayList<>();
        switch (logPath.getLogPathType()) {
            case FILE:
                list.add(HDFSUtil.getReaderObject(nameNode, logPath.getLogPath(), FILE_SYSTEM_TYPE));
                break;
            case DIRECTORY:
                List<String> files = listFiles();
                if (files.size() > 0) {
                    for (String path : files) {
                        list.add(HDFSUtil.getReaderObject(nameNode, path, FILE_SYSTEM_TYPE));
                    }
                }
                break;
            default:
                return null;
        }
        return list;
    }

    @Override
    public void setMapReduceEventLogPath() {

    }

}
