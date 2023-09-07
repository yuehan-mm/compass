package com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality;


import com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.bean.ScriptInfo;
import com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.service.SqlDiagnoseOffLineService;

import java.util.List;


public class OffLineTaskLauncher {
    public static void main(String[] args) {
        work2();
    }

    public static void work1() {
        List<ScriptInfo> scriptInfos = SqlDiagnoseOffLineService.getScriptList();

        System.out.println("Parse Script Start Total Rows : " + scriptInfos.size());
        SqlDiagnoseOffLineService.parseScript(scriptInfos);
        System.out.println("PARSE Success Rows : " + (scriptInfos.size() - SqlDiagnoseOffLineService.failCount.get()));
        System.out.println("PARSE Fail Rows : " + SqlDiagnoseOffLineService.failCount.get());

        System.out.println("Get Report Start");
        SqlDiagnoseOffLineService.buildReport(scriptInfos);
        System.out.println("Get Report End");

        System.out.println("Write Excel Start");
        SqlDiagnoseOffLineService.writeExcel(scriptInfos);
        System.out.println("Write Excel End");
    }

    public static void work2() {
        List<ScriptInfo> scriptInfos = SqlDiagnoseOffLineService.getScriptList();

        System.out.println("Parse Script Start Total Rows : " + scriptInfos.size());
        SqlDiagnoseOffLineService.parseScript(scriptInfos);
        System.out.println("PARSE Success Rows : " + (scriptInfos.size() - SqlDiagnoseOffLineService.failCount.get()));
        System.out.println("PARSE Fail Rows : " + SqlDiagnoseOffLineService.failCount.get());

        System.out.println("Get Report Start");
        SqlDiagnoseOffLineService.buildReport(scriptInfos);
        System.out.println("Get Report End");

        System.out.println("Write Table Start");
        SqlDiagnoseOffLineService.deleteData();
        SqlDiagnoseOffLineService.writeTable(scriptInfos);
        System.out.println("Write Table End");
    }
}
