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

package com.oppo.cloud.parser.service.job.parser;

import com.oppo.cloud.common.constant.LogType;
import com.oppo.cloud.common.constant.ProgressState;
import com.oppo.cloud.common.domain.job.LogPath;
import com.oppo.cloud.common.domain.oneclick.OneClickProgress;
import com.oppo.cloud.common.domain.oneclick.ProgressInfo;
import com.oppo.cloud.common.util.spring.SpringBeanUtil;
import com.oppo.cloud.common.util.textparser.ParserAction;
import com.oppo.cloud.common.util.textparser.ParserManager;
import com.oppo.cloud.common.util.textparser.TextParser;
import com.oppo.cloud.parser.config.CustomConfig;
import com.oppo.cloud.parser.config.DiagnosisConfig;
import com.oppo.cloud.parser.config.ThreadPoolConfig;
import com.oppo.cloud.parser.domain.job.CommonResult;
import com.oppo.cloud.parser.domain.job.ParserParam;
import com.oppo.cloud.parser.domain.job.ReadFileInfo;
import com.oppo.cloud.parser.domain.job.SparkExecutorLogParserResult;
import com.oppo.cloud.parser.domain.reader.ReaderObject;
import com.oppo.cloud.parser.service.reader.IReader;
import com.oppo.cloud.parser.service.reader.LogReaderFactory;
import com.oppo.cloud.parser.service.writer.ElasticWriter;
import com.oppo.cloud.parser.utils.GCReportUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.io.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

@Slf4j
public class SparkExecutorLogParser extends CommonTextParser implements IParser {

    private final ParserParam param;

    private final boolean isOneClick;

    private final Executor parserThreadPool;

    private final List<String> jvmTypeList;

    public SparkExecutorLogParser(ParserParam param) {
        this.param = param;
        this.isOneClick = param.getTaskParam().getIsOneClick();
        parserThreadPool = (ThreadPoolTaskExecutor) SpringBeanUtil.getBean(ThreadPoolConfig.PARSER_THREAD_POOL);
        jvmTypeList = (List<String>) SpringBeanUtil.getBean(CustomConfig.GC_CONFIG);

    }

    public CommonResult run() {
        updateParserProgress(ProgressState.PROCESSING, 0, 0);
        CommonResult<List<SparkExecutorLogParserResult>> commonResult = new CommonResult<>();
        List<SparkExecutorLogParserResult> gcReports = new ArrayList<>();
        for (LogPath logPath : this.param.getLogPaths()) {
            List<ReaderObject> readerObjects;
            try {
                IReader reader = LogReaderFactory.create(logPath);
                readerObjects = reader.getReaderObjects();
            } catch (Exception e) {
                log.error("SparkExecutorLogParser fail:" + e.getMessage());
                continue;
            }
            if (readerObjects.size() > 0) {
                updateParserProgress(ProgressState.PROCESSING, 0, readerObjects.size());
                List<SparkExecutorLogParserResult> results = handleReaderObjects(readerObjects);
                gcReports.addAll(results);
            }
        }
        updateParserProgress(ProgressState.SUCCEED, 0, 0);
        commonResult.setLogType(this.param.getLogType());
        commonResult.setResult(gcReports);
        return commonResult;

    }

    private List<SparkExecutorLogParserResult> handleReaderObjects(List<ReaderObject> readerObjects) {
        List<CompletableFuture<SparkExecutorLogParserResult>> futures = new ArrayList<>();
        for (ReaderObject readerObject : readerObjects) {
            CompletableFuture<SparkExecutorLogParserResult> future =
                    CompletableFuture.supplyAsync(() -> handleReaderObject(readerObject), parserThreadPool);
            futures.add(future);
        }
        List<SparkExecutorLogParserResult> results = new ArrayList<>();
        int i = 0;
        for (Future<SparkExecutorLogParserResult> result : futures) {
            SparkExecutorLogParserResult sp = null;
            try {
                sp = result.get();
            } catch (Exception e) {
                log.error("Exception:", e);
            }
            updateParserProgress(ProgressState.PROCESSING, i++, readerObjects.size());
            if (sp != null) {
                results.add(sp);
            }
        }
        return results;
    }

    private SparkExecutorLogParserResult handleReaderObject(ReaderObject readerObject) {
        String logType = getLogType(readerObject.getLogPath());
        SparkExecutorLogParserResult result = null;
        try {
            result = parseAction(logType, readerObject);
        } catch (Exception e) {
            log.error("Exception:", e);
        }
        if (result != null && result.getActionMap() != null) {
            List<String> categories = ElasticWriter.getInstance()
                    .saveParserActions(logType, readerObject.getLogPath(), this.param, result.getActionMap());
            result.setCategories(categories);
        }
        return result;
    }


    private SparkExecutorLogParserResult parseAction(String logType, ReaderObject readerObject) throws Exception {
        SparkExecutorLogParserResult result = parseRootAction(logType, readerObject);
        for (Map.Entry<String, ParserAction> action : result.getActionMap().entrySet()) {
            ParserManager.parseChildActions(action.getValue());
        }
        return result;
    }

    private SparkExecutorLogParserResult parseRootAction(String logType, ReaderObject readerObject) throws Exception {
        String sqlCommand = "";
        Map<String, ReadFileInfo> readFileInfo = new HashMap<>();
        List<ParserAction> actions = DiagnosisConfig.getInstance().getActions(logType);
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

            this.parseFileInfo(line, readFileInfo);
            if (line.contains("For Compass SQL base64 : ")) {
                sqlCommand = this.parseSQLInfo(line);
            }

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

        readerObject.close();


        SparkExecutorLogParserResult result = new SparkExecutorLogParserResult();
        result.setActionMap(headTextParser.getResults());
        if (gcLogMap.size() > 0) {
            result.setGcReports(GCReportUtil.generateGCReports(gcLogMap, readerObject.getLogPath()));
        }
        result.setLogPath(readerObject.getLogPath());
        result.setReadFileInfo(readFileInfo);
        result.setSqlCommand(sqlCommand);
        return result;
    }

    /**
     * 23/08/22 15:08:40 INFO service.SparkSqlEngine: For Compass SQL base64 : dXNlIGRsX2hkb3BfdHh0OwppbnNlcnQgaW50byB0
     * dF9oZG9wX2JkbXBqdHNqX3ZhX3RfZ3JvdXBiYiBzZWxlY3QgKiBmcm9tIHR0X2hkb3BfYmRtcGp0c2pfdmFfdF9ncm91cCANCg==
     *
     * @param line
     * @return
     */
    private String parseSQLInfo(String line) {
        String sqlCommandBase64 = line.split("For Compass SQL base64 : ")[1];
        Base64.Decoder decoder = Base64.getDecoder();
        String sqlCommand = new String(decoder.decode(sqlCommandBase64));
        log.info("parseSQLInfo sqlCommandBase64:" + sqlCommandBase64);
        return sqlCommand;
    }

    /**
     * spark的这个地方，有两种读方式，所以做了区别
     * spark 把 parquet/orc 看作一种，其他看作一种
     * parquet/orc 这种表是
     * datasources.FileScanRDD: Reading File path:
     * hdfs://nameservice1/user/hive/warehouse/dh_hic.db/dim_center/
     * part-00001-16d3e056-c270-4824-8907-4438ab0d415c-c000.snappy.parquet, range: 0-10849, partition values: [empty row]
     * 其他 这种表是Input split
     * rdd.HadoopRDD: Input split:
     * hdfs://nameservice1/user/hive/warehouse/dl_hdop_txt.db/tt_hdop_bdmpjtsj_va_t_group/datax__7255aeb7_236f_472e_99f1_d42f1e60ee26:0+4916
     *
     * @param line
     * @param readFileInfo
     */
    private void parseFileInfo(String line, Map<String, ReadFileInfo> readFileInfo) {
        if (line.contains("HadoopRDD: Input split:")) {
            String[] infos = line.split("hdfs://nameservice1")[1].split(":");
            Long start = Long.valueOf(infos[1].split("\\+")[0]);
            Long offSets = Long.valueOf(infos[1].split("\\+")[1]);
            Long end = start + offSets;
            if (!readFileInfo.containsKey(infos[0]) || end > readFileInfo.get(infos[0]).getMaxOffsets()) {
                readFileInfo.put(infos[0], new ReadFileInfo(infos[0], end, "rdd.HadoopRDD"));
            }
        } else if (line.contains("datasources.FileScanRDD: Reading File path:")) {
            String[] infos = line.split("hdfs://nameservice1")[1].split(",");
            Long end = Long.valueOf(infos[1].split(":")[1].split("-")[1]);
            if (!readFileInfo.containsKey(infos[0]) || end > readFileInfo.get(infos[0]).getMaxOffsets()) {
                readFileInfo.put(infos[0], new ReadFileInfo(infos[0], end, "rdd.HadoopRDD"));
            }
        }
    }


    private String getLogType(String logPath) {
        if (logPath.contains(this.param.getTaskParam().getTaskApp().getAmHost())) {
            return LogType.SPARK_DRIVER.getName();
        }
        return LogType.SPARK_EXECUTOR.getName();
    }


    public void updateParserProgress(ProgressState state, Integer progress, Integer count) {
        if (!this.isOneClick) {
            return;
        }
        OneClickProgress oneClickProgress = new OneClickProgress();
        oneClickProgress.setAppId(this.param.getTaskParam().getTaskApp().getApplicationId());
        oneClickProgress.setLogType(LogType.SPARK_EXECUTOR);
        ProgressInfo executorProgress = new ProgressInfo();
        executorProgress.setCount(count);
        executorProgress.setProgress(progress);
        executorProgress.setState(state);
        oneClickProgress.setProgressInfo(executorProgress);
        super.update(oneClickProgress);
    }
}
