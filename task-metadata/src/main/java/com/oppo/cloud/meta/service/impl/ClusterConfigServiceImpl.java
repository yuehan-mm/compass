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

package com.oppo.cloud.meta.service.impl;

import com.alibaba.fastjson2.JSON;
import com.oppo.cloud.common.constant.Constant;
import com.oppo.cloud.common.domain.cluster.hadoop.NameNodeConf;
import com.oppo.cloud.common.domain.cluster.hadoop.YarnConf;
import com.oppo.cloud.common.domain.cluster.yarn.ClusterInfo;
import com.oppo.cloud.common.service.RedisService;
import com.oppo.cloud.meta.config.HadoopConfig;
import com.oppo.cloud.meta.service.IClusterConfigService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * YARN、SPARK集群地址配置信息
 */
@Slf4j
@Service
public class ClusterConfigServiceImpl implements IClusterConfigService {

    @Resource
    private RedisService redisService;

    @Resource
    private HadoopConfig config;

    @Resource(name = "restTemplate")
    private RestTemplate restTemplate;

    private static final String YARN_CLUSTER_INFO = "http://%s/ws/v1/cluster/info";

    private static final String YARN_CONF = "http://%s/conf";

    private Pattern defaultFSPattern = Pattern.compile(".*<name>fs.defaultFS</name><value>(?<defaultFS>.*?)</value>.*",
            Pattern.DOTALL);

    private Pattern remoteDirPattern = Pattern.compile(".*<name>yarn.nodemanager.remote-app-log-dir</name><value>" +
            "(?<remoteDir>.*?)</value>.*", Pattern.DOTALL);

    /**
     * 获取spark history server列表
     */
    @Override
    public List<String> getSparkHistoryServers() {
        return config.getSpark().getSparkHistoryServer();
    }

    @Override
    public NameNodeConf getHdfsConf() {
        List<NameNodeConf> confList = config.getNamenodes();
        return (confList != null && confList.size() > 0) ? confList.get(0) : null;
    }

    /**
     * 获取yarn rm列表
     */
    @Override
    public Map<String, String> getYarnClusters() {
        List<YarnConf> yarnConfList = config.getYarn();
        List<NameNodeConf> namenodes = config.getNamenodes();
        Map<String, String> yarnClusters = new HashMap<>();
        for (int i = 0; i < yarnConfList.size(); i++) {
            YarnConf yarnConf = yarnConfList.get(i);
            NameNodeConf nameNodeConf = namenodes.get(i);
            String activeHost = getRmActiveHost(yarnConf.getResourceManager(), nameNodeConf);
            if (StringUtils.isEmpty(activeHost)) {
                continue;
            }
            yarnClusters.put(activeHost, yarnConf.getClusterName());
        }
        return yarnClusters;
    }

    public String getRmActiveHost(List<String> list, NameNodeConf nameNodeConf) {
        for (String host : list) {
            String clusterInfoUrl = String.format(YARN_CLUSTER_INFO, host);
            HttpClient httpClient = HttpClient.getInstance(host, true, nameNodeConf.getLoginUser(),
                    nameNodeConf.getKeytabPath(), nameNodeConf.getKrb5Conf());
            CloseableHttpResponse response ;
            try {
                response = httpClient.get(clusterInfoUrl);
            } catch (Exception e) {
                log.error("Exception:", e);
                continue;
            }
            ClusterInfo clusterInfo;
            try {
                clusterInfo = JSON.parseObject(EntityUtils.toString(response.getEntity()), ClusterInfo.class);
            } catch (Exception e) {
                log.error("Exception:", e);
                continue;
            }
            if (clusterInfo == null) {
                log.error("get active host null:{}", clusterInfoUrl);
                continue;
            }
            log.info("YarnRmInfo-->{}:{}", host, clusterInfo.getClusterInfo().getHaState());
            if ("ACTIVE".equals(clusterInfo.getClusterInfo().getHaState())) {
                return host;
            }
        }
        return null;
    }

    /**
     * 更新集群信息
     */
    @Override
    public void updateClusterConfig() {

        log.info("clusterConfig:{}", config);
        // cache spark history server
        List<String> sparkHistoryServerList = config.getSpark().getSparkHistoryServer();
        log.info("{}:{}", Constant.SPARK_HISTORY_SERVERS, sparkHistoryServerList);
        redisService.set(Constant.SPARK_HISTORY_SERVERS, JSON.toJSONString(sparkHistoryServerList));

        // cache yarn server
        List<YarnConf> yarnConfList = config.getYarn();
        // resourceManager 对应的 jobHistoryServer
        Map<String, String> rmJhsMap = new HashMap<>();

        yarnConfList.forEach(
                clusterInfo -> clusterInfo
                        .getResourceManager()
                        .forEach(
                                rm -> rmJhsMap.put(rm, clusterInfo.getJobHistoryServer())
                        )
        );

        redisService.set(Constant.YARN_CLUSTERS, JSON.toJSONString(sparkHistoryServerList));
        log.info("{}:{}", Constant.YARN_CLUSTERS, yarnConfList);
        redisService.set(Constant.RM_JHS_MAP, JSON.toJSONString(rmJhsMap));
        log.info("{}:{}", Constant.RM_JHS_MAP, rmJhsMap);
        updateJHSConfig(yarnConfList);
    }

    /**
     * 更新配置中jobhistoryserver hdfs路径信息
     */
    public void updateJHSConfig(List<YarnConf> list) {
        for (int i = 0; i < list.size(); i++) {
            String host = list.get(i).getJobHistoryServer();
//            String hdfsPath = getHDFSPath(host, config.getNamenodes().get(i));
//
//            if (StringUtils.isEmpty(hdfsPath)) {
//                log.error("get {}, hdfsPath empty", host);
//                continue;
//            }

//            String key = Constant.JHS_HDFS_PATH + host;
//            log.info("cache hdfsPath:{},{}", key, hdfsPath);
//            redisService.set(key, hdfsPath);

            try{
                Document jobHistoryConfig = getJobHistoryConfig(host, config.getNamenodes().get(i));
                String remoteDir = getValueFromDocument(jobHistoryConfig, "yarn.nodemanager.remote-app-log-dir");
                String defaultFS = getValueFromDocument(jobHistoryConfig, "fs.defaultFS");
                String eventDir = getValueFromDocument(jobHistoryConfig, "mapreduce.jobhistory.done-dir");

                redisService.set(Constant.JHS_HDFS_PATH + host, defaultFS + remoteDir);
                redisService.set(Constant.JHS_EVENT_PATH + host, defaultFS + eventDir );
            }catch (Exception e){

            }


        }
    }


    private String getValueFromDocument(org.dom4j.Document document, String key){
        final Pattern qutoPattern = Pattern.compile("\\$\\{(.*?)\\}");

        Element rootElement = document.getRootElement();
        for (Iterator i = rootElement.elementIterator(); i.hasNext(); ) {
            Element next = (Element) i.next();
            if (next.element("name").getData().equals(key)) {
                String value = String.valueOf(next.element("value").getData());

                while (true){
                    Matcher matcher = qutoPattern.matcher(value);
                    if(!matcher.find()) return value;

                    String findKey = matcher.group(1);
                    String findValue = getValueFromDocument(document, findKey);

                    value = value.replaceFirst("\\$\\{(.*?)\\}", findValue);
                }

            }
        }

        throw new IllegalArgumentException("cannot found key with : " + key);
    }

    /***
     * 获取 JobHistoryServer 配置
     * @param ip
     * @param nameNodeConf
     * @return
     */
    public Document getJobHistoryConfig(String ip, NameNodeConf nameNodeConf){

        String url = String.format(YARN_CONF, ip);

        log.info("getHDFSPath:{}", url);

        HttpClient httpClient = HttpClient.getInstance(ip, true, nameNodeConf.getLoginUser(),
                nameNodeConf.getKeytabPath(), nameNodeConf.getKrb5Conf());

        try {
            CloseableHttpResponse response = httpClient.get(url);
            return DocumentHelper.parseText(EntityUtils.toString(response.getEntity()));
        } catch (Exception e) {
            log.error("send request fail url: " + url);
            log.error("{}", e);
            return null;
        }

    }

    /**
     * 获取jobhistoryserver hdfs路径信息
     */
    public String getHDFSPath(String ip, NameNodeConf nameNodeConf) {
        String url = String.format(YARN_CONF, ip);

        log.info("getHDFSPath:{}", url);

        HttpClient httpClient = HttpClient.getInstance(ip, true, nameNodeConf.getLoginUser(),
                nameNodeConf.getKeytabPath(), nameNodeConf.getKrb5Conf());
        CloseableHttpResponse response;
        try {
            response = httpClient.get(url);
        } catch (Exception e) {
            log.error("send request fail url: " + url);
            return null;
        }

        String remoteDir = "";
        String defaultFS = "";
        org.dom4j.Document document;
        try {
            document = DocumentHelper.parseText(EntityUtils.toString(response.getEntity()));
        } catch (DocumentException e) {
            log.error("DocumentException : " + e.getMessage());
            return null;
        } catch (IOException e) {
            log.error("IOException: " + e.getMessage());
            return null;
        }


        try{
            remoteDir = getValueFromDocument(document, "yarn.nodemanager.remote-app-log-dir");
        }catch (IllegalArgumentException e){}

        try{
            defaultFS = getValueFromDocument(document, "fs.defaultFS");
        }catch (IllegalArgumentException e){}





        if (StringUtils.isEmpty(remoteDir)) {
            log.error("remoteDirEmpty:{}", url);
            return null;
        }
        if (!remoteDir.contains("hdfs")) {
            if (StringUtils.isEmpty(defaultFS)) {
                log.error("defaultFSEmpty:{}", url);
                return null;
            }
            remoteDir = defaultFS + remoteDir;
        }
        log.info("getHDFSPath url: {},remoteDir: {}", url, remoteDir);
        return remoteDir;
    }


}
