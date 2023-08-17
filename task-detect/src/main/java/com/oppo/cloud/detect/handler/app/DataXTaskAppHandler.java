package com.oppo.cloud.detect.handler.app;

import com.oppo.cloud.common.constant.LogPathType;
import com.oppo.cloud.common.constant.LogType;
import com.oppo.cloud.common.domain.elasticsearch.TaskApp;
import com.oppo.cloud.common.domain.job.LogPath;
import com.oppo.cloud.model.TaskApplication;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 * DATAX 作业构建
 */
public class DataXTaskAppHandler {

    public void handler(TaskApplication taskApplication, TaskApp taskApp, String hdfsBasePath) {


        // TODO 运行耗时:  0.00s
        //  内存消耗:  0.00 GB·s
        //  CPU消耗:  0.00 vcore·s
        taskApp.setElapsedTime(9999.9);
        taskApp.setStartTime(new Date());
        taskApp.setFinishTime(new Date(System.currentTimeMillis() + 199999));
        taskApp.setMemorySeconds(9999.9);
        taskApp.setVcoreSeconds(9999.9);
        // 兼容测试环境
        String protocol = hdfsBasePath.contains("oss") ? "oss" : "hdfs";
        taskApp.addLogPath(LogType.DATAX_RUNTIME,
                new LogPath(protocol, LogPathType.DIRECTORY, getDataXLogFuzzyPath(taskApplication, hdfsBasePath)));
    }

    public String getDataXLogFuzzyPath(TaskApplication taskApplication, String hdfsBasePath) {
        List<String> paths = new ArrayList<>();
        paths.add(hdfsBasePath);
        paths.add(taskApplication.getFlowName());
        paths.add(taskApplication.getTaskName());
        paths.add(convertTime(taskApplication.getExecuteTime())
                .replace(":", "_")
                .replace("+", "_")
                + "-"
                + (taskApplication.getRetryTimes() + 1)
                + "*"
        );
        return String.join("/", paths);
    }

    private String convertTime(Date date) {
        DateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd'T'HH_mm_ssXXX");
        sdf2.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf2.format(date).replaceAll("Z", "_00_00");
    }
}
