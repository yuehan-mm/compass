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

package com.oppo.cloud.parser.utils;

import com.oppo.cloud.common.domain.cluster.hadoop.NameNodeConf;
import com.oppo.cloud.parser.domain.reader.ReaderObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.*;

/**
 * HDFSUtil
 */
@Slf4j
public class HDFSUtil {

    private static final String HDFS_SCHEME = "hdfs://";
    private static final String FILE_SYSTEM_TYPE_OSS = "OSS";
    /**
     * get hdfs NameNode
     */
    public static NameNodeConf getNameNode(Map<String, NameNodeConf> nameNodeMap, String filePath) {
        for (String nameService : nameNodeMap.keySet()) {
            NameNodeConf nameNodeConf = nameNodeMap.get(nameService);
            for (String pathKey : nameNodeConf.getMatchPathKeys()) {
                if (filePath.contains(pathKey)) {
                    return nameNodeConf;
                }
            }
            if (nameService != null && !nameService.isEmpty() && filePath.contains(nameService)) {
                return nameNodeConf;
            }
        }
        return null;
    }

    private static FileSystem getFileSystem(NameNodeConf nameNodeConf, String fsType) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        conf.addResource(new Path(nameNodeConf.getCoresite()));
        conf.addResource(new Path(nameNodeConf.getHdfssite()));
        if (fsType.equals(FILE_SYSTEM_TYPE_OSS)) {
            conf.set("fs.defaultFS", "oss://haier-hdop-presto");
        }
        if (nameNodeConf.isEnableKerberos()) {
            return getAuthenticationFileSystem(nameNodeConf, conf);
        }
        return FileSystem.get(conf);
    }

    private static FileSystem getAuthenticationFileSystem(NameNodeConf nameNodeConf, Configuration conf) throws Exception {
        conf.set("hadoop.security.authorization", "true");
        conf.set("hadoop.security.authentication", "kerberos");
        System.setProperty("java.security.krb5.conf", nameNodeConf.getKrb5Conf());
        conf.set("dfs.namenode.kerberos.principal.pattern", nameNodeConf.getPrincipalPattern());
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(nameNodeConf.getLoginUser(), nameNodeConf.getKeytabPath());
        UserGroupInformation ugi = UserGroupInformation.getLoginUser();
        return ugi.doAs((PrivilegedExceptionAction<FileSystem>) () -> FileSystem.newInstance(conf));
    }

    public static String[] readLines(NameNodeConf nameNode, String logPath, String fsType) throws Exception {
        FSDataInputStream fsDataInputStream = null;
        try {
            FileSystem fs = HDFSUtil.getFileSystem(nameNode, fsType);
            fsDataInputStream = fs.open(new Path(logPath));
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            // 64kb
            byte[] buffer = new byte[65536];
            int byteRead;

            while ((byteRead = fsDataInputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, byteRead);
            }
            byte[] contents = outputStream.toByteArray();
            String s = new String(contents, StandardCharsets.UTF_8);
            return s.split("\n");
        } catch (Exception e) {
            throw new Exception();
        } finally {
            if (Objects.nonNull(fsDataInputStream)) {
                fsDataInputStream.close();
            }
        }
    }

    public static ReaderObject getReaderObject(NameNodeConf nameNode, String path, String fsType) throws Exception {
        FileSystem fs = HDFSUtil.getFileSystem(nameNode, fsType);
        FSDataInputStream fsDataInputStream = fs.open(new Path(path));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream));
        ReaderObject readerObject = new ReaderObject();
        readerObject.setLogPath(path);
        readerObject.setBufferedReader(bufferedReader);
        readerObject.setFs(fs);
        return readerObject;
    }

    public static List<String> listFiles(NameNodeConf nameNode, String path, String fsType) throws Exception {
        FileSystem fs = HDFSUtil.getFileSystem(nameNode, fsType);
        RemoteIterator<LocatedFileStatus> it = fs.listFiles(new Path(path), true);
        List<String> result = new ArrayList<>();
        while (it.hasNext()) {
            LocatedFileStatus locatedFileStatus = it.next();
            result.add(locatedFileStatus.getPath().toString());
        }
        fs.close();
        return result;
    }

}
