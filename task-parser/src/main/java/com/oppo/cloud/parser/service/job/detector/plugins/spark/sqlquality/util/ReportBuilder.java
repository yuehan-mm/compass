package com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.util;


import com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.bean.ScriptDiagnoseDetail;
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

    public static SqlReport buildReport(List<ScriptDiagnoseDetail> scriptInfos) {
        SqlReport sqlReport = new SqlReport();
        scriptInfos.stream().parallel().forEach(scriptInfo -> {

            buildUnionCountReport(scriptInfo.getSqlUnionValue(), sqlReport.getUnionCountReport());
            buildGroupByCountReport(scriptInfo.getSqlGroupByValue(), sqlReport.getGroupByCountReport());
            buildJoinCountReport(scriptInfo.getSqlJoinValue(), sqlReport.getJoinCountReport());
            buildOrderByCountReport(scriptInfo.getSqlOrderByValue(), sqlReport.getOrderByCountReport());

            buildLengthReport(scriptInfo.getSqlLengthValue(), sqlReport.getSqlLengthReport());
            buildTableRefCountReport(scriptInfo.getSqlTableRefValue(), sqlReport.getTableRefCountReport());
            buildTableReadCountReport(scriptInfo.getSqlTableReadValue(), sqlReport.getTableReadCountReport());

            buildScanFileCountReport(scriptInfo.getSqlScanFileCountValue(), sqlReport.getScanFileCountReport());
            buildScanFileSizeReport(scriptInfo.getSqlScanFileSizeValue(), sqlReport.getScanFileSizeReport());
            buildScanSmallFileCountReport(scriptInfo.getSqlScanSmallFileCountValue(), sqlReport.getScanSmallFileCountReport());
            buildScanPartitionCountReport(scriptInfo.getSqlScanPartitionCountValue(), sqlReport.getScanPartitionCountReport());

        });
        return sqlReport;
    }


    private static void buildJoinCountReport(Integer joinCount, SqlReport.JoinCountReport joinCountReport) {
        if (joinCount >= 50) {
            joinCountReport.getGt50_().getValue().incrementAndGet();
        } else if (joinCount >= 40) {
            joinCountReport.getGt40_50().getValue().incrementAndGet();
        } else if (joinCount >= 30) {
            joinCountReport.getGt30_40().getValue().incrementAndGet();
        } else if (joinCount >= 20) {
            joinCountReport.getGt20_30().getValue().incrementAndGet();
        } else if (joinCount >= 10) {
            joinCountReport.getGt10_20().getValue().incrementAndGet();
        } else if (joinCount >= 5) {
            joinCountReport.getGt5_10().getValue().incrementAndGet();
        } else {
            joinCountReport.get_le5().getValue().incrementAndGet();
        }
    }

    private static void buildOrderByCountReport(Integer orderByCount, SqlReport.OrderByCountReport orderByCountReport) {
        if (orderByCount >= 50) {
            orderByCountReport.getGt50_().getValue().incrementAndGet();
        } else if (orderByCount >= 40) {
            orderByCountReport.getGt40_50().getValue().incrementAndGet();
        } else if (orderByCount >= 30) {
            orderByCountReport.getGt30_40().getValue().incrementAndGet();
        } else if (orderByCount >= 20) {
            orderByCountReport.getGt20_30().getValue().incrementAndGet();
        } else if (orderByCount >= 10) {
            orderByCountReport.getGt10_20().getValue().incrementAndGet();
        } else if (orderByCount >= 5) {
            orderByCountReport.getGt5_10().getValue().incrementAndGet();
        } else {
            orderByCountReport.get_le5().getValue().incrementAndGet();
        }
    }

    private static void buildGroupByCountReport(Integer groupByCount, SqlReport.GroupByCountReport groupByCountReport) {
        if (groupByCount >= 50) {
            groupByCountReport.getGt50_().getValue().incrementAndGet();
        } else if (groupByCount >= 40) {
            groupByCountReport.getGt40_50().getValue().incrementAndGet();
        } else if (groupByCount >= 30) {
            groupByCountReport.getGt30_40().getValue().incrementAndGet();
        } else if (groupByCount >= 20) {
            groupByCountReport.getGt20_30().getValue().incrementAndGet();
        } else if (groupByCount >= 10) {
            groupByCountReport.getGt10_20().getValue().incrementAndGet();
        } else if (groupByCount >= 5) {
            groupByCountReport.getGt5_10().getValue().incrementAndGet();
        } else {
            groupByCountReport.get_le5().getValue().incrementAndGet();
        }
    }

    private static void buildLengthReport(Integer sqlLength, SqlReport.SqlLengthReport sqlLengthReport) {
        if (sqlLength >= 50000) {
            sqlLengthReport.getGt5000_().getValue().incrementAndGet();
        } else if (sqlLength >= 40000) {
            sqlLengthReport.getGt40000_50000().getValue().incrementAndGet();
        } else if (sqlLength >= 30000) {
            sqlLengthReport.getGt30000_40000().getValue().incrementAndGet();
        } else if (sqlLength >= 20000) {
            sqlLengthReport.getGt20000_30000().getValue().incrementAndGet();
        } else if (sqlLength >= 10000) {
            sqlLengthReport.getGt10000_20000().getValue().incrementAndGet();
        } else if (sqlLength >= 5000) {
            sqlLengthReport.getGt5000_10000().getValue().incrementAndGet();
        } else if (sqlLength >= 2000) {
            sqlLengthReport.getGt2000_5000().getValue().incrementAndGet();
        } else if (sqlLength >= 1000) {
            sqlLengthReport.getGt1000_2000().getValue().incrementAndGet();
        } else if (sqlLength >= 500) {
            sqlLengthReport.getGt500_1000().getValue().incrementAndGet();
        } else {
            sqlLengthReport.get_gt500().getValue().incrementAndGet();
        }

    }

    private void buildSqlScoreReport(double score, SqlReport.SqlScoreReport sqlScoreReport) {
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


    private static void buildUnionCountReport(int count, SqlReport.UnionCountReport unionCountReport) {
        if (count >= 50) {
            unionCountReport.getGt50_().getValue().incrementAndGet();
        } else if (count >= 40) {
            unionCountReport.getGt40_50().getValue().incrementAndGet();
        } else if (count >= 30) {
            unionCountReport.getGt30_40().getValue().incrementAndGet();
        } else if (count >= 20) {
            unionCountReport.getGt20_30().getValue().incrementAndGet();
        } else if (count >= 10) {
            unionCountReport.getGt10_20().getValue().incrementAndGet();
        } else if (count >= 5) {
            unionCountReport.getGt5_10().getValue().incrementAndGet();
        } else {
            unionCountReport.get_le5().getValue().incrementAndGet();
        }
    }

    private static void buildTableRefCountReport(int count, SqlReport.TableRefCountReport tableRefCountReport) {
        if (count >= 50) {
            tableRefCountReport.getGt50_().getValue().incrementAndGet();
        } else if (count >= 40) {
            tableRefCountReport.getGt40_50().getValue().incrementAndGet();
        } else if (count >= 30) {
            tableRefCountReport.getGt30_40().getValue().incrementAndGet();
        } else if (count >= 20) {
            tableRefCountReport.getGt20_30().getValue().incrementAndGet();
        } else if (count >= 10) {
            tableRefCountReport.getGt10_20().getValue().incrementAndGet();
        } else if (count >= 5) {
            tableRefCountReport.getGt5_10().getValue().incrementAndGet();
        } else {
            tableRefCountReport.get_le5().getValue().incrementAndGet();
        }
    }

    private static void buildTableReadCountReport(int count, SqlReport.TableUseCountReport tableUseCountReport) {
        if (count >= 50) {
            tableUseCountReport.getGt50_().getValue().incrementAndGet();
        } else if (count >= 40) {
            tableUseCountReport.getGt40_50().getValue().incrementAndGet();
        } else if (count >= 30) {
            tableUseCountReport.getGt30_40().getValue().incrementAndGet();
        } else if (count >= 20) {
            tableUseCountReport.getGt20_30().getValue().incrementAndGet();
        } else if (count >= 10) {
            tableUseCountReport.getGt10_20().getValue().incrementAndGet();
        } else if (count >= 5) {
            tableUseCountReport.getGt5_10().getValue().incrementAndGet();
        } else {
            tableUseCountReport.get_le5().getValue().incrementAndGet();
        }
    }

    private static void buildScanFileCountReport(int count, SqlReport.ScanFileCountReport scanFileCountReport) {
        if (count >= 50) {
            scanFileCountReport.getGt50_().getValue().incrementAndGet();
        } else if (count >= 40) {
            scanFileCountReport.getGt40_50().getValue().incrementAndGet();
        } else if (count >= 30) {
            scanFileCountReport.getGt30_40().getValue().incrementAndGet();
        } else if (count >= 20) {
            scanFileCountReport.getGt20_30().getValue().incrementAndGet();
        } else if (count >= 10) {
            scanFileCountReport.getGt10_20().getValue().incrementAndGet();
        } else if (count >= 5) {
            scanFileCountReport.getGt5_10().getValue().incrementAndGet();
        } else {
            scanFileCountReport.get_le5().getValue().incrementAndGet();
        }
    }

    private static void buildScanFileSizeReport(long size, SqlReport.ScanFileSizeReport scanFileSizeReport) {
        if (size >= 1024 * 1024 * 1024 * 50) {
            scanFileSizeReport.getGt50G_().getValue().incrementAndGet();
        } else if (size >= 1024 * 1024 * 1024 * 10) {
            scanFileSizeReport.getGt10G_50G().getValue().incrementAndGet();
        } else if (size >= 1024 * 1024 * 1024 * 5) {
            scanFileSizeReport.getGt5G_10G().getValue().incrementAndGet();
        } else if (size >= 1024 * 1024 * 1024) {
            scanFileSizeReport.getGt1G_5G().getValue().incrementAndGet();
        } else if (size >= 1024 * 1024 * 300) {
            scanFileSizeReport.getGt300M_1G().getValue().incrementAndGet();
        } else if (size >= 1024 * 1024 * 100) {
            scanFileSizeReport.getGt100M_300M().getValue().incrementAndGet();
        } else if (size >= 1024 * 1024 * 50) {
            scanFileSizeReport.getGt50M_100M().getValue().incrementAndGet();
        } else if (size >= 1024 * 1024 * 10) {
            scanFileSizeReport.getGt10M_50M().getValue().incrementAndGet();
        } else {
            scanFileSizeReport.get_le10M().getValue().incrementAndGet();
        }
    }

    private static void buildScanSmallFileCountReport(Integer count, SqlReport.ScanSmallFileCountReport scanSmallFileCountReport) {
        if (count >= 50) {
            scanSmallFileCountReport.getGt50_().getValue().incrementAndGet();
        } else if (count >= 40) {
            scanSmallFileCountReport.getGt40_50().getValue().incrementAndGet();
        } else if (count >= 30) {
            scanSmallFileCountReport.getGt30_40().getValue().incrementAndGet();
        } else if (count >= 20) {
            scanSmallFileCountReport.getGt20_30().getValue().incrementAndGet();
        } else if (count >= 10) {
            scanSmallFileCountReport.getGt10_20().getValue().incrementAndGet();
        } else if (count >= 5) {
            scanSmallFileCountReport.getGt5_10().getValue().incrementAndGet();
        } else {
            scanSmallFileCountReport.get_le5().getValue().incrementAndGet();
        }
    }

    private static void buildScanPartitionCountReport(Integer count, SqlReport.ScanPartitionCountReport scanPartitionCountReport) {
        if (count >= 50) {
            scanPartitionCountReport.getGt50_().getValue().incrementAndGet();
        } else if (count >= 40) {
            scanPartitionCountReport.getGt40_50().getValue().incrementAndGet();
        } else if (count >= 30) {
            scanPartitionCountReport.getGt30_40().getValue().incrementAndGet();
        } else if (count >= 20) {
            scanPartitionCountReport.getGt20_30().getValue().incrementAndGet();
        } else if (count >= 10) {
            scanPartitionCountReport.getGt10_20().getValue().incrementAndGet();
        } else if (count >= 5) {
            scanPartitionCountReport.getGt5_10().getValue().incrementAndGet();
        } else {
            scanPartitionCountReport.get_le5().getValue().incrementAndGet();
        }
    }

}
