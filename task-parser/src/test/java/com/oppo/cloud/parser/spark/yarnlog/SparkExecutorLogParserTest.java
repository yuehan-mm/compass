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

package com.oppo.cloud.parser.spark.yarnlog;

import com.alibaba.fastjson2.JSON;
import com.oppo.cloud.common.constant.LogType;
import com.oppo.cloud.common.util.textparser.ParserAction;
import com.oppo.cloud.common.util.textparser.ParserActionUtil;
import com.oppo.cloud.common.util.textparser.ParserManager;
import com.oppo.cloud.common.util.textparser.TextParser;
import com.oppo.cloud.parser.domain.job.SparkExecutorLogParserResult;
import com.oppo.cloud.parser.domain.reader.ReaderObject;
import com.oppo.cloud.parser.utils.GCReportUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class SparkExecutorLogParserTest {


    private static FileSystem fs;
    private static Configuration hdfsconf = new Configuration();

    static {
        System.setProperty("java.security.krb5.conf", "C:\\Users\\22047328\\Desktop\\keberos\\青岛测试\\krb5.conf");
        hdfsconf.set("fs.defaultFS", "hdfs://nameservice1");
        hdfsconf.set("dfs.nameservices", "nameservice1");
        hdfsconf.set("dfs.client.failover.proxy.provider.nameservice1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        hdfsconf.set("dfs.ha.automatic-failover.enabled.nameservice1", "true");
        hdfsconf.set("ha.zookeeper.quorum", "qdedhtest0:2181,qdedhtest1:2181,qdedhtest2:2181");
        hdfsconf.set("dfs.ha.namenodes.nameservice1", "namenode30,namenode172");
        hdfsconf.set("dfs.namenode.rpc-address.nameservice1.namenode30", "qdedhtest0:8020");
        hdfsconf.set("dfs.namenode.rpc-address.nameservice1.namenode172", "qdedhtest1:8020");
        hdfsconf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        hdfsconf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        hdfsconf.setBoolean("hadoop.security.authentication", true);
        hdfsconf.set("hadoop.security.authentication", "kerberos");
        hdfsconf.set("dfs.namenode.kerberos.principal.pattern", "hdfs/*@HAIER.COM");
        UserGroupInformation.setConfiguration(hdfsconf);
        try {
            UserGroupInformation.loginUserFromKeytab("admin", "C:\\Users\\22047328\\Desktop\\keberos\\青岛测试\\admin.keytab");
            fs = FileSystem.get(hdfsconf);
            System.out.println("############ hdfs krb 认证通过");
        } catch (Exception e) {
            System.out.println(e.getMessage());
            throw new RuntimeException("get FileSystem fail");
        }
    }

    String ss = "    [\n" +
            "      {\n" +
            "        \"action\": \"containerFailed\",\n" +
            "        \"desc\": \"Container失败\",\n" +
            "        \"category\": \"otherException\",\n" +
            "        \"step\": 1,\n" +
            "        \"skip\": false,\n" +
            "        \"parserType\": \"DEFAULT\",\n" +
            "        \"parserTemplate\": {\n" +
            "          \"heads\": [\n" +
            "            \"^(?<datetime>\\\\d{2,4}.\\\\d{2}.\\\\d{2}\\\\D+\\\\d{2}:\\\\d{2}:\\\\d{2}).+Container marked as failed: (?<container>\\\\S+) on.+Exit status: (?<exitStatus>.+)\\\\..+Diagnostics:.+$\"\n" +
            "          ],\n" +
            "          \"middles\": [],\n" +
            "          \"tails\": [\n" +
            "            \"^(?<datetime>\\\\d{2,4}.\\\\d{2}.\\\\d{2}\\\\D+\\\\d{2}:\\\\d{2}:\\\\d{2}).+$\"\n" +
            "          ]\n" +
            "        },\n" +
            "        \"groupNames\": [\n" +
            "          \"datetime\",\n" +
            "          \"container\",\n" +
            "          \"exitStatus\"\n" +
            "        ],\n" +
            "        \"children\": []\n" +
            "      },\n" +
            "      {\n" +
            "        \"action\": \"stageFailed\",\n" +
            "        \"desc\": \"Stage失败\",\n" +
            "        \"category\": \"otherException\",\n" +
            "        \"step\": 2,\n" +
            "        \"skip\": false,\n" +
            "        \"parserType\": \"DEFAULT\",\n" +
            "        \"parserTemplate\": {\n" +
            "          \"heads\": [\n" +
            "            \"^(?<datetime>\\\\d{2,4}.\\\\d{2}.\\\\d{2}\\\\D+\\\\d{2}:\\\\d{2}:\\\\d{2}).+ in stage (?<stage>\\\\S+) failed\\\\s+(?<failedNum>\\\\S+)\\\\s+times;.+$\"\n" +
            "          ],\n" +
            "          \"middles\": [],\n" +
            "          \"tails\": []\n" +
            "        },\n" +
            "        \"groupNames\": [\n" +
            "          \"datetime\",\n" +
            "          \"stage\",\n" +
            "          \"failedNum\"\n" +
            "        ],\n" +
            "        \"children\": []\n" +
            "      },\n" +
            "      {\n" +
            "        \"action\": \"jobFailedOrAbortedException\",\n" +
            "        \"desc\": \"任务失败或退出异常\",\n" +
            "        \"category\": \"otherException\",\n" +
            "        \"step\": 3,\n" +
            "        \"skip\": false,\n" +
            "        \"parserType\": \"DEFAULT\",\n" +
            "        \"parserTemplate\": {\n" +
            "          \"heads\": [\n" +
            "            \"^(?<datetime>\\\\d{2,4}.\\\\d{2}.\\\\d{2}\\\\D+\\\\d{2}:\\\\d{2}:\\\\d{2}).+Failed to run job.+$\",\n" +
            "            \"^(?<datetime>\\\\d{2,4}.\\\\d{2}.\\\\d{2}\\\\D+\\\\d{2}:\\\\d{2}:\\\\d{2}).+Job aborted due to.+$\"\n" +
            "          ],\n" +
            "          \"middles\": [],\n" +
            "          \"tails\": [\n" +
            "            \"^(?<datetime>\\\\d{2,4}.\\\\d{2}.\\\\d{2}\\\\D+\\\\d{2}:\\\\d{2}:\\\\d{2}).+$\"\n" +
            "          ]\n" +
            "        },\n" +
            "        \"groupNames\": [\n" +
            "          \"datetime\"\n" +
            "        ],\n" +
            "        \"children\": [\n" +
            "          {\n" +
            "            \"action\": \"fileNotFoundException\",\n" +
            "            \"desc\": \"文件未找到异常\",\n" +
            "            \"category\": \"sqlFailed\",\n" +
            "            \"step\": 1,\n" +
            "            \"skip\": false,\n" +
            "            \"parserType\": \"DEFAULT\",\n" +
            "            \"parserTemplate\": {\n" +
            "              \"heads\": [\n" +
            "                \".+File does not exist.+\"\n" +
            "              ],\n" +
            "              \"middles\": [],\n" +
            "              \"tails\": []\n" +
            "            },\n" +
            "            \"groupNames\": [],\n" +
            "            \"children\": []\n" +
            "          },\n" +
            "          {\n" +
            "            \"action\": \"failedRunJobNoTablePermission\",\n" +
            "            \"desc\": \"用户没有权限\",\n" +
            "            \"category\": \"sqlFailed\",\n" +
            "            \"step\": 2,\n" +
            "            \"skip\": false,\n" +
            "            \"parserType\": \"DEFAULT\",\n" +
            "            \"parserTemplate\": {\n" +
            "              \"heads\": [\n" +
            "                \"^.+the required privilege is hive://(?<user>.*):user@(?<zone>.*)/hive/(?<database>.*)/(?<table>.*)\\\\?option=(?<option>.*)$\"\n" +
            "              ],\n" +
            "              \"middles\": [],\n" +
            "              \"tails\": []\n" +
            "            },\n" +
            "            \"groupNames\": [\n" +
            "              \"user\",\n" +
            "              \"zone\",\n" +
            "              \"database\",\n" +
            "              \"table\",\n" +
            "              \"option\"\n" +
            "            ],\n" +
            "            \"children\": []\n" +
            "          },\n" +
            "          {\n" +
            "            \"action\": \"sqlSyncError\",\n" +
            "            \"desc\": \"sql语法错误,外部表必须添加外部地址\",\n" +
            "            \"category\": \"sqlFailed\",\n" +
            "            \"step\": 3,\n" +
            "            \"skip\": false,\n" +
            "            \"parserType\": \"DEFAULT\",\n" +
            "            \"parserTemplate\": {\n" +
            "              \"heads\": [\n" +
            "                \"^.*Operation not allowed.*CREATE EXTERNAL TABLE must be accompanied by LOCATION.*$\"\n" +
            "              ],\n" +
            "              \"middles\": [],\n" +
            "              \"tails\": []\n" +
            "            },\n" +
            "            \"groupNames\": [],\n" +
            "            \"children\": []\n" +
            "          },\n" +
            "          {\n" +
            "            \"action\": \"broadcastsTimeout\",\n" +
            "            \"desc\": \"广播超时\",\n" +
            "            \"category\": \"otherException\",\n" +
            "            \"step\": 4,\n" +
            "            \"skip\": false,\n" +
            "            \"parserType\": \"DEFAULT\",\n" +
            "            \"parserTemplate\": {\n" +
            "              \"heads\": [\n" +
            "                \"^.+org\\\\.apache\\\\.spark\\\\.SparkException.*Could not execute broadcast in.*secs.*$\"\n" +
            "              ],\n" +
            "              \"middles\": [],\n" +
            "              \"tails\": []\n" +
            "            },\n" +
            "            \"groupNames\": [],\n" +
            "            \"children\": []\n" +
            "          },\n" +
            "          {\n" +
            "            \"action\": \"blockMissingException\",\n" +
            "            \"desc\": \"块丢失异常\",\n" +
            "            \"category\": \"otherException\",\n" +
            "            \"step\": 5,\n" +
            "            \"skip\": false,\n" +
            "            \"parserType\": \"DEFAULT\",\n" +
            "            \"parserTemplate\": {\n" +
            "              \"heads\": [\n" +
            "                \"^.+org.apache.hadoop.hdfs.BlockMissingException.*Could not obtain block.*$\"\n" +
            "              ],\n" +
            "              \"middles\": [],\n" +
            "              \"tails\": []\n" +
            "            },\n" +
            "            \"groupNames\": [],\n" +
            "            \"children\": []\n" +
            "          },\n" +
            "          {\n" +
            "            \"action\": \"outOfHeapOOM\",\n" +
            "            \"desc\": \"shuffle阶段获取数据时堆外内存溢出\",\n" +
            "            \"category\": \"memoryOverflow\",\n" +
            "            \"step\": 6,\n" +
            "            \"skip\": false,\n" +
            "            \"parserType\": \"DEFAULT\",\n" +
            "            \"parserTemplate\": {\n" +
            "              \"heads\": [\n" +
            "                \"^.+org\\\\.apache\\\\.spark\\\\.SparkException.*Job aborted due to stage failure.+Container killed by YARN for exceeding memory limits.+$\"\n" +
            "              ],\n" +
            "              \"middles\": [],\n" +
            "              \"tails\": []\n" +
            "            },\n" +
            "            \"groupNames\": [\n" +
            "              \"datetime\"\n" +
            "            ],\n" +
            "            \"children\": []\n" +
            "          }\n" +
            "        ]\n" +
            "      },\n" +
            "      {\n" +
            "        \"action\": \"shuffleFetchFailed\",\n" +
            "        \"desc\": \"shuffle连接不上\",\n" +
            "        \"category\": \"shuffleFailed\",\n" +
            "        \"step\": 4,\n" +
            "        \"skip\": false,\n" +
            "        \"parserType\": \"DEFAULT\",\n" +
            "        \"parserTemplate\": {\n" +
            "          \"heads\": [\n" +
            "            \"^(?<datetime>\\\\d{2,4}.\\\\d{2}.\\\\d{2}\\\\D+\\\\d{2}:\\\\d{2}:\\\\d{2}).+TaskSetManager.+FetchFailed.+$\"\n" +
            "          ],\n" +
            "          \"middles\": [],\n" +
            "          \"tails\": []\n" +
            "        },\n" +
            "        \"groupNames\": [\n" +
            "          \"datetime\"\n" +
            "        ],\n" +
            "        \"children\": []\n" +
            "      },\n" +
            "      {\n" +
            "        \"action\": \"outOfMemoryError\",\n" +
            "        \"desc\": \"内存溢出\",\n" +
            "        \"category\": \"memoryOverflow\",\n" +
            "        \"step\": 5,\n" +
            "        \"skip\": false,\n" +
            "        \"parserType\": \"DEFAULT\",\n" +
            "        \"parserTemplate\": {\n" +
            "          \"heads\": [\n" +
            "            \"^(?<datetime>\\\\d{2,4}.\\\\d{2}.\\\\d{2}\\\\D+\\\\d{2}:\\\\d{2}:\\\\d{2}).+OutOfMemoryError.+$\"\n" +
            "          ],\n" +
            "          \"middles\": [],\n" +
            "          \"tails\": [\n" +
            "            \"^(?<datetime>\\\\d{2,4}.\\\\d{2}.\\\\d{2}\\\\D+\\\\d{2}:\\\\d{2}:\\\\d{2}).+$\"\n" +
            "          ]\n" +
            "        },\n" +
            "        \"groupNames\": [\n" +
            "          \"datetime\"\n" +
            "        ],\n" +
            "        \"children\": []\n" +
            "      },\n" +
            "      {\n" +
            "        \"action\": \"otherError\",\n" +
            "        \"desc\": \"其他错误信息\",\n" +
            "        \"category\": \"otherException\",\n" +
            "        \"step\": 6,\n" +
            "        \"skip\": false,\n" +
            "        \"parserType\": \"DEFAULT\",\n" +
            "        \"parserTemplate\": {\n" +
            "          \"heads\": [\n" +
            "            \"^(?<datetime>\\\\d{2,4}.\\\\d{2}.\\\\d{2}\\\\D+\\\\d{2}:\\\\d{2}:\\\\d{2}).+ERROR.+$\"\n" +
            "          ],\n" +
            "          \"middles\": [],\n" +
            "          \"tails\": []\n" +
            "        },\n" +
            "        \"groupNames\": [\n" +
            "          \"datetime\"\n" +
            "        ],\n" +
            "        \"children\": []\n" +
            "      }\n" +
            "    ]";


    private final List<String> jvmTypeList;

    public SparkExecutorLogParserTest() {
        jvmTypeList = new ArrayList<>();
        jvmTypeList.add("- Java HotSpot");
        jvmTypeList.add("- OpenJDK");
    }


    public static void main(String[] args) throws Exception {
        new SparkExecutorLogParserTest().parseAction();
    }


    private SparkExecutorLogParserResult parseAction() throws Exception {
        // TODO
        SparkExecutorLogParserResult result = parseRootAction("driver", getReaderObject("/tmp/logs/hive/logs/application_1679449548714_8680/qdedhtest2_8041"));
        for (Map.Entry<String, ParserAction> action : result.getActionMap().entrySet()) {
            ParserManager.parseChildActions(action.getValue());
        }
        return result;
    }

    public List<ParserAction> getActions() {
        List<ParserAction> actions = new ArrayList<>();
        try {
            actions = JSON.parseArray(ss, ParserAction.class);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        actions = ParserActionUtil.verifyParserActions(actions);
        return actions;
    }

    private SparkExecutorLogParserResult parseRootAction(String logType, ReaderObject readerObject) throws Exception {
        // rules.json,actions -> 对象
        List<ParserAction> actions = JSON.parseArray(ss, ParserAction.class);
        Map<Integer, InputStream> gcLogMap = new HashMap<>();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        boolean isGCLog = false;
        boolean isStderr = false;

        TextParser headTextParser = new TextParser(actions);
        BufferedReader bufferedReader = readerObject.getBufferedReader();
        while (true) {
            String line;
            try {
                line = bufferedReader.readLine();
            } catch (IOException e) {
                log.error(e.getMessage());
                continue;
            }
            if (line == null) {
                break;
            }
            headTextParser.parse(line);

            // get gc log
            if (line.contains("stderr")) {
                isGCLog = false;
                if (LogType.SPARK_DRIVER.getName().equals(logType) && byteArrayOutputStream.size() > 0) {
                    gcLogMap.put(0, new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
                }
                isStderr = true;
            }

            if (jvmTypeList != null && !isGCLog) {
                for (String jvm : jvmTypeList) {
                    if (line.contains(jvm)) {
                        isGCLog = true;
                        line = jvm + line.split(jvm)[1];
                        break;
                    }
                }
            }

            if (isGCLog) {
                line += "\n";
                byteArrayOutputStream.write(line.getBytes());
            }
            if (isStderr && line.contains("Starting executor ID")) {
                String id = line.split("ID")[1].split("on")[0].trim();
                if (byteArrayOutputStream.size() > 0) {
                    String gcLog = byteArrayOutputStream.toString();
                    log.debug("gcLog:{}\n{}", readerObject.getLogPath(), gcLog);
                    gcLogMap.put(Integer.valueOf(id), new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
                    byteArrayOutputStream = new ByteArrayOutputStream();
                }
            }

        }
        if (readerObject.getFs() != null) {
            readerObject.getFs().close();
        }

        SparkExecutorLogParserResult result = new SparkExecutorLogParserResult();
        result.setActionMap(headTextParser.getResults());
        log.info("-----" + gcLogMap);
        if (gcLogMap.size() > 0) {
            result.setGcReports(GCReportUtil.generateGCReports(gcLogMap, readerObject.getLogPath()));
        }
        result.setLogPath(readerObject.getLogPath());

        return result;
    }


    public static ReaderObject getReaderObject(String path) throws Exception {
        FSDataInputStream fsDataInputStream = fs.open(new Path(path));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream));
        ReaderObject readerObject = new ReaderObject();
        readerObject.setLogPath(path);
        readerObject.setBufferedReader(bufferedReader);
        readerObject.setFs(fs);
        return readerObject;
    }


}
