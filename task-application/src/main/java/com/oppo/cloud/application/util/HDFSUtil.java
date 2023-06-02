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

package com.oppo.cloud.application.util;

import com.oppo.cloud.common.domain.cluster.hadoop.NameNodeConf;
import com.sun.jna.Callback;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Hdfs工具类
 */
@Slf4j
public class HDFSUtil {

    /**
     * 获取Namnode, 根据配置matchPathKeys是否被包含在路径关键字中
     */
    public static NameNodeConf getNameNode(Map<String, NameNodeConf> nameNodeMap, String filePath) {
        for (String nameService : nameNodeMap.keySet()) {
            NameNodeConf nameNodeConf = nameNodeMap.get(nameService);
            for (String pathKey : nameNodeConf.getMatchPathKeys()) {
                if (filePath.contains(pathKey)) {
                    return nameNodeConf;
                }
            }
        }
        return null;
    }


    private static FileSystem getFileSystem(NameNodeConf nameNodeConf, String filePath) throws Exception {
        Configuration conf = new Configuration();
        conf.addResource(new Path(nameNodeConf.getCoresite()));
        conf.addResource(new Path(nameNodeConf.getHdfssite()));

        if(conf.get("hadoop.security.authorization").equals("true") &&
            conf.get("hadoop.security.authentication").equals("kerberos")){
            System.setProperty("java.security.krb5.conf", nameNodeConf.getKrb5Conf());
//            conf.set("dfs.namenode.kerberos.principal.pattern", nameNodeConf.getPrincipalPattern());
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(nameNodeConf.getLoginUser(), nameNodeConf.getKeytabPath());
            UserGroupInformation ugi = UserGroupInformation.getLoginUser();
            return ugi.doAs((PrivilegedExceptionAction<FileSystem>) () -> FileSystem.newInstance(URI.create(filePath), conf));
        }else{
            return FileSystem.newInstance(URI.create(filePath), conf);
        }
    }

    /**
     * 读取文件，返回日志内容
     */
    public static void readLines(NameNodeConf nameNodeConf, String filePath, HDFSReaderCallback callback) throws Exception {
        FSDataInputStream fsDataInputStream = null;
        FileSystem fs = null;
        try {
            fs = HDFSUtil.getFileSystem(nameNodeConf, filePath);
            fsDataInputStream = fs.open(new Path(filePath));
            BufferedReader reader = new BufferedReader(new InputStreamReader(fsDataInputStream));


//            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            for(String strLine = reader.readLine(); strLine != null; strLine = reader.readLine()){
                callback.call(strLine);
            }
//            // 64kb
//            byte[] buffer = new byte[65536];
//            int byteRead;
//
//            while ((byteRead = fsDataInputStream.read(buffer)) != -1) {
//                outputStream.write(buffer, 0, byteRead);
//            }
//
//            byte[] bytes = outputStream.toByteArray();
//            String datas = new String(bytes, StandardCharsets.UTF_8);
//            return datas.split("\n");
        } catch (Exception e) {
            throw new Exception(String.format("failed to read file: %s, err: %s", filePath, e.getMessage()));
        } finally {
            if (Objects.nonNull(fsDataInputStream)) {
                fsDataInputStream.close();
            }
            if (fs != null) {
                fs.close();
            }
        }
    }

    /**
     * 通配符获取文件列表, 带*通配
     */
    public static List<String> filesPattern(NameNodeConf nameNodeConf, String filePath) throws Exception {
//        filePath = checkLogPath(nameNodeConf, filePath);
        FileSystem fs = HDFSUtil.getFileSystem(nameNodeConf, filePath);
        FileStatus[] fileStatuses = fs.globStatus(new Path(filePath));
        List<String> result = new ArrayList<>();
        if (fileStatuses == null) {
            return result;
        }

        for (FileStatus fileStatus : fileStatuses) {
            if (fs.exists(fileStatus.getPath())) {
                result.add(fileStatus.getPath().toString());
            }
        }
        fs.close();
        return result;
    }

    private static String checkLogPath(NameNodeConf nameNode, String logPath) {

        return logPath;

//        if (logPath.contains(HDFS_SCHEME) || logPath.contains(OSS_SCHEME)) {
//            return logPath;
//        }
//        return String.format("%s%s%s", HDFS_SCHEME, nameNode.getNameservices(), logPath);
    }
}
