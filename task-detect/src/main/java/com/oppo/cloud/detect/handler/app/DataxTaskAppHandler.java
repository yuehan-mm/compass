package com.oppo.cloud.detect.handler.app;

import com.oppo.cloud.common.constant.LogPathType;
import com.oppo.cloud.common.constant.LogType;
import com.oppo.cloud.common.domain.elasticsearch.TaskApp;
import com.oppo.cloud.common.domain.job.LogPath;
import com.oppo.cloud.common.service.RedisService;
import com.oppo.cloud.detect.service.ElasticSearchService;
import com.oppo.cloud.model.TaskApplication;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 *  DATAX 作业构建
 */
@Component
public class DataxTaskAppHandler implements TaskAppHandler {

    @Value("${spring.hdfs.base-path}")
    private String HDFS_BASE_PATH;

    @Override
    public void handler(TaskApplication taskApplication, TaskApp taskApp,
                        ElasticSearchService elasticSearchService, RedisService redisService) {
        taskApp.addLogPath(LogType.DATAX_RUNTIME,
                new LogPath("oss", LogPathType.DIRECTORY, getDataXLogPath(taskApplication)));
    }

    public String getDataXLogPath(TaskApplication taskApplication) {
        List<String> paths = new ArrayList<>();
        paths.add(HDFS_BASE_PATH);
        paths.add(taskApplication.getFlowName());
        paths.add(taskApplication.getTaskName());
        paths.add(convertTime(taskApplication.getExecuteTime())
                .replace(":", "_")
                .replace("+", "_")
                + "-"
                + taskApplication.getRetryTimes()
        );
        return String.join("/", paths);
    }

    private String convertTime(Date date) {
        DateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd'T'HH_mm_ssXXX");
        sdf2.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf2.format(date).replaceAll("Z", "_00_00");
    }
}
