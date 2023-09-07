
package com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.util;


import com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.bean.DiagnoseResult;
import com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.bean.ScriptInfo;
import com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.bean.SqlReport;
import lombok.Data;

import java.util.List;

@Data
public class ReportBuilder {
    List<ScriptInfo> scriptInfos;

    public ReportBuilder(List<ScriptInfo> scriptInfos) {
        this.scriptInfos = scriptInfos;
    }

    public SqlReport buildReport() {
        SqlReport sqlReport = new SqlReport();
        scriptInfos.stream().parallel().forEach(scriptInfo -> {
            DiagnoseResult diagnoseResult = scriptInfo.getDiagnoseResult();
            buildUnionCountReport(diagnoseResult.getUnionCount(), sqlReport.getUnionCountReport());
            buildTableRefCountReport(diagnoseResult.getRefTableMap().size(), sqlReport.getTableRefCountReport());
            buildTableUseCountReport(diagnoseResult.getRefTableMap()
                    .keySet().stream()
                    .map(x -> diagnoseResult.getRefTableMap().get(x))
                    .reduce((x, y) -> x + y)
                    .orElse(0), sqlReport.getTableUseCountReport());
            buildSqlScoreReport(scriptInfo.getScore(), sqlReport.getSqlScoreReport());
            buildLengthReport(diagnoseResult.getSqlLength(), sqlReport.getSqlLengthReport());
            buildGroupByCountReport(diagnoseResult.getGroupByCount(), sqlReport.getGroupByCountReport());
            buildOrderByCountReport(diagnoseResult.getOrderByCount(), sqlReport.getOrderByCountReport());
            buildJoinCountReport(diagnoseResult.getJoinCount(), sqlReport.getJoinCountReport());
        });
        return sqlReport;
    }

    private void buildJoinCountReport(Integer joinCount, SqlReport.JoinCountReport joinCountReport) {
        if (joinCount >= 50) {
            joinCountReport.getGt50_().incrementAndGet();
        } else if (joinCount >= 40) {
            joinCountReport.getGt40_50().incrementAndGet();
        } else if (joinCount >= 30) {
            joinCountReport.getGt30_40().incrementAndGet();
        } else if (joinCount >= 20) {
            joinCountReport.getGt20_30().incrementAndGet();
        } else if (joinCount >= 10) {
            joinCountReport.getGt10_20().incrementAndGet();
        } else if (joinCount >= 5) {
            joinCountReport.getGt5_10().incrementAndGet();
        } else {
            joinCountReport.get_le5().incrementAndGet();
        }
    }

    private void buildOrderByCountReport(Integer orderByCount, SqlReport.OrderByCountReport orderByCountReport) {
        if (orderByCount >= 50) {
            orderByCountReport.getGt50_().incrementAndGet();
        } else if (orderByCount >= 40) {
            orderByCountReport.getGt40_50().incrementAndGet();
        } else if (orderByCount >= 30) {
            orderByCountReport.getGt30_40().incrementAndGet();
        } else if (orderByCount >= 20) {
            orderByCountReport.getGt20_30().incrementAndGet();
        } else if (orderByCount >= 10) {
            orderByCountReport.getGt10_20().incrementAndGet();
        } else if (orderByCount >= 5) {
            orderByCountReport.getGt5_10().incrementAndGet();
        } else {
            orderByCountReport.get_le5().incrementAndGet();
        }
    }

    private void buildGroupByCountReport(Integer groupByCount, SqlReport.GroupByCountReport groupByCountReport) {
        if (groupByCount >= 50) {
            groupByCountReport.getGt50_().incrementAndGet();
        } else if (groupByCount >= 40) {
            groupByCountReport.getGt40_50().incrementAndGet();
        } else if (groupByCount >= 30) {
            groupByCountReport.getGt30_40().incrementAndGet();
        } else if (groupByCount >= 20) {
            groupByCountReport.getGt20_30().incrementAndGet();
        } else if (groupByCount >= 10) {
            groupByCountReport.getGt10_20().incrementAndGet();
        } else if (groupByCount >= 5) {
            groupByCountReport.getGt5_10().incrementAndGet();
        } else {
            groupByCountReport.get_le5().incrementAndGet();
        }
    }

    private void buildLengthReport(Integer sqlLength, SqlReport.SqlLengthReport sqlLengthReport) {
        if (sqlLength >= 50000) {
            sqlLengthReport.getGt5000_().incrementAndGet();
        } else if (sqlLength >= 40000) {
            sqlLengthReport.getGt40000_50000().incrementAndGet();
        } else if (sqlLength >= 30000) {
            sqlLengthReport.getGt30000_40000().incrementAndGet();
        } else if (sqlLength >= 20000) {
            sqlLengthReport.getGt20000_30000().incrementAndGet();
        } else if (sqlLength >= 10000) {
            sqlLengthReport.getGt10000_20000().incrementAndGet();
        } else if (sqlLength >= 5000) {
            sqlLengthReport.getGt5000_10000().incrementAndGet();
        } else if (sqlLength >= 2000) {
            sqlLengthReport.getGt2000_5000().incrementAndGet();
        } else if (sqlLength >= 1000) {
            sqlLengthReport.getGt1000_2000().incrementAndGet();
        } else if (sqlLength >= 500) {
            sqlLengthReport.getGt500_1000().incrementAndGet();
        } else {
            sqlLengthReport.get_gt500().incrementAndGet();
        }

    }

    private void buildSqlScoreReport(Integer score, SqlReport.SqlScoreReport sqlScoreReport) {
        if (score >= 85) {
            sqlScoreReport.getGt85_().incrementAndGet();
        } else if (score >= 60) {
            sqlScoreReport.getGt60_85().incrementAndGet();
        } else if (score >= 0) {
            sqlScoreReport.getGt0_60().incrementAndGet();
        } else if (score >= -100) {
            sqlScoreReport.getGtF100_0().incrementAndGet();
        } else if (score >= -200) {
            sqlScoreReport.getGtF200_F100().incrementAndGet();
        } else if (score >= -500) {
            sqlScoreReport.getGtF500_F200().incrementAndGet();
        } else if (score >= -1000) {
            sqlScoreReport.getGtF1000_F500().incrementAndGet();
        } else if (score >= -2000) {
            sqlScoreReport.getGtF2000_F1000().incrementAndGet();
        } else if (score >= -3000) {
            sqlScoreReport.getGtF3000_F2000().incrementAndGet();
        } else if (score >= -4000) {
            sqlScoreReport.getGtF4000_F3000().incrementAndGet();
        } else if (score >= -5000) {
            sqlScoreReport.getGtF5000_F4000().incrementAndGet();
        } else {
            sqlScoreReport.get_gtF5000().incrementAndGet();
        }
    }


    private void buildUnionCountReport(int count, SqlReport.UnionCountReport unionCountReport) {
        if (count >= 50) {
            unionCountReport.getGt50_().incrementAndGet();
        } else if (count >= 40) {
            unionCountReport.getGt40_50().incrementAndGet();
        } else if (count >= 30) {
            unionCountReport.getGt30_40().incrementAndGet();
        } else if (count >= 20) {
            unionCountReport.getGt20_30().incrementAndGet();
        } else if (count >= 10) {
            unionCountReport.getGt10_20().incrementAndGet();
        } else if (count >= 5) {
            unionCountReport.getGt5_10().incrementAndGet();
        } else {
            unionCountReport.get_le5().incrementAndGet();
        }
    }

    private void buildTableRefCountReport(int count, SqlReport.TableRefCountReport tableRefCountReport) {
        if (count >= 50) {
            tableRefCountReport.getGt50_().incrementAndGet();
        } else if (count >= 40) {
            tableRefCountReport.getGt40_50().incrementAndGet();
        } else if (count >= 30) {
            tableRefCountReport.getGt30_40().incrementAndGet();
        } else if (count >= 20) {
            tableRefCountReport.getGt20_30().incrementAndGet();
        } else if (count >= 10) {
            tableRefCountReport.getGt10_20().incrementAndGet();
        } else if (count >= 5) {
            tableRefCountReport.getGt5_10().incrementAndGet();
        } else {
            tableRefCountReport.get_le5().incrementAndGet();
        }
    }

    private void buildTableUseCountReport(int count, SqlReport.TableUseCountReport tableUseCountReport) {
        if (count >= 50) {
            tableUseCountReport.getGt50_().incrementAndGet();
        } else if (count >= 40) {
            tableUseCountReport.getGt40_50().incrementAndGet();
        } else if (count >= 30) {
            tableUseCountReport.getGt30_40().incrementAndGet();
        } else if (count >= 20) {
            tableUseCountReport.getGt20_30().incrementAndGet();
        } else if (count >= 10) {
            tableUseCountReport.getGt10_20().incrementAndGet();
        } else if (count >= 5) {
            tableUseCountReport.getGt5_10().incrementAndGet();
        } else {
            tableUseCountReport.get_le5().incrementAndGet();
        }
    }
}
