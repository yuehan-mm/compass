package com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality;


import com.alibaba.fastjson2.JSON;
import com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.bean.ScriptDiagnoseDetail;
import com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.bean.ScriptInfo;
import com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.service.SqlDiagnoseOffLineService;
import com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.util.ReportBuilder;
import org.apache.commons.lang.time.FastDateFormat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class OffLineTaskLauncher {
    public static void main(String[] args) {
        doReport();
    }

    public static void doReport() {
        System.out.println("Get ScriptList Start");
        List<ScriptDiagnoseDetail> scriptInfos = SqlDiagnoseOffLineService.getDiagnoseResultList();

        System.out.println("Get Report Start");
        Map<String, List<ReportBuilder.StaticElement>> stringListHashMap = ReportBuilder.buildReport(scriptInfos);
        System.out.println(JSON.toJSONString(stringListHashMap));
        System.out.println("Get Report End");

    }

    public static void doWork() {
        FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd");
        String insertDataTime = fastDateFormat.format(System.currentTimeMillis() + 1000 * 60 * 60 * 24);
        String removeDataTime = fastDateFormat.format(System.currentTimeMillis() - 1000 * 60 * 60 * 24 * 7);

        System.out.println("Get ScriptList Start");
        List<ScriptInfo> scriptInfos = SqlDiagnoseOffLineService.getScriptList();

        System.out.println("Parse Start Total Rows : " + scriptInfos.size());
        SqlDiagnoseOffLineService.parseScript(scriptInfos);
        System.out.println("Parse Success Rows : " + (scriptInfos.size() - SqlDiagnoseOffLineService.failCount.get()));
        System.out.println("Parse Fail Rows : " + SqlDiagnoseOffLineService.failCount.get());

        System.out.println("Write Table Start");
        SqlDiagnoseOffLineService.deleteData(removeDataTime);
        SqlDiagnoseOffLineService.writeTable(scriptInfos, insertDataTime);
        System.out.println("Write Table End");
    }
}
