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

package com.oppo.cloud.parser.service.writer;

import com.oppo.cloud.common.util.spring.SpringBeanUtil;
import com.oppo.cloud.parser.config.HdopDBConfig;
import com.oppo.cloud.parser.domain.job.TaskParam;
import com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.DiagnoseContent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * elasticsearch writer
 */
@Slf4j
@Service
public class MysqlWriter {

    public Connection client;

    public String url;

    public String username;

    public String password;


    private MysqlWriter() {
        HdopDBConfig yml = (HdopDBConfig) SpringBeanUtil.getBean(HdopDBConfig.class);
        url = yml.getUrl();
        username = yml.getUsername();
        password = yml.getPassword();
        try {
            Class.forName("com.mysql.jdbc.Driver");
            client = DriverManager.getConnection(url, username, password);
            log.info("get mysql connection success. url:{}, username:{}, password: {} .", url, username, password);
        } catch (Exception e) {
            log.error("get mysql connection fail. msg: {}, url:{}, username:{}, password: {} .", e.getMessage(), url, username, password);
        }
    }

    public static MysqlWriter getInstance() {
        return Mysql.INSTANCE.getInstance();
    }


    public void updateOffLineData(DiagnoseContent scriptInfo, TaskParam taskParam) {
        log.info("updateOffLineData----" + url);
    }

    private enum Mysql {

        INSTANCE;

        private final MysqlWriter singleton;

        Mysql() {
            singleton = new MysqlWriter();
        }

        public MysqlWriter getInstance() {
            return singleton;
        }
    }
}
