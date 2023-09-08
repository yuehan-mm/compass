package com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality;


import com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.bean.ScriptInfo;
import com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.service.SqlDiagnoseOffLineService;
import org.apache.commons.lang.time.FastDateFormat;

import java.util.List;


public class OffLineTaskLauncher {
    public static void main(String[] args) {
        doWork();
    }

    public static void doReport() {
        System.out.println("Get ScriptList Start");
        List<ScriptInfo> scriptInfos = SqlDiagnoseOffLineService.getScriptList();

        System.out.println("Parse Start Total Rows : " + scriptInfos.size());
        SqlDiagnoseOffLineService.parseScript(scriptInfos);
        System.out.println("Parse Success Rows : " + (scriptInfos.size() - SqlDiagnoseOffLineService.failCount.get()));
        System.out.println("Parse Fail Rows : " + SqlDiagnoseOffLineService.failCount.get());

        System.out.println("Get Report Start");
        SqlDiagnoseOffLineService.buildReport(scriptInfos);
        System.out.println("Get Report End");

        System.out.println("Write Excel Start");
        SqlDiagnoseOffLineService.writeExcel(scriptInfos);
        System.out.println("Write Excel End");
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
