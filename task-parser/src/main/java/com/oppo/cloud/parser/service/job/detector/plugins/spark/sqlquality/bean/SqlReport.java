package com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.bean;

import lombok.Data;

import java.util.concurrent.atomic.AtomicInteger;

@Data
public class SqlReport {

    UnionCountReport unionCountReport = new UnionCountReport();
    TableRefCountReport tableRefCountReport = new TableRefCountReport();
    TableUseCountReport tableUseCountReport = new TableUseCountReport();
    SqlScoreReport sqlScoreReport = new SqlScoreReport();
    SqlLengthReport sqlLengthReport = new SqlLengthReport();
    GroupByCountReport groupByCountReport = new GroupByCountReport();
    JoinCountReport joinCountReport = new JoinCountReport();
    OrderByCountReport orderByCountReport = new OrderByCountReport();


    @Data
    public static class UnionCountReport {
        private AtomicInteger gt50_ = new AtomicInteger();
        private AtomicInteger gt40_50 = new AtomicInteger();
        private AtomicInteger gt30_40 = new AtomicInteger();
        private AtomicInteger gt20_30 = new AtomicInteger();
        private AtomicInteger gt10_20 = new AtomicInteger();
        private AtomicInteger gt5_10 = new AtomicInteger();
        private AtomicInteger _le5 = new AtomicInteger();
    }

    @Data
    public static class TableRefCountReport {
        private AtomicInteger gt50_ = new AtomicInteger();
        private AtomicInteger gt40_50 = new AtomicInteger();
        private AtomicInteger gt30_40 = new AtomicInteger();
        private AtomicInteger gt20_30 = new AtomicInteger();
        private AtomicInteger gt10_20 = new AtomicInteger();
        private AtomicInteger gt5_10 = new AtomicInteger();
        private AtomicInteger _le5 = new AtomicInteger();
    }

    @Data
    public static class TableUseCountReport {
        private AtomicInteger gt50_ = new AtomicInteger();
        private AtomicInteger gt40_50 = new AtomicInteger();
        private AtomicInteger gt30_40 = new AtomicInteger();
        private AtomicInteger gt20_30 = new AtomicInteger();
        private AtomicInteger gt10_20 = new AtomicInteger();
        private AtomicInteger gt5_10 = new AtomicInteger();
        private AtomicInteger _le5 = new AtomicInteger();
    }

    @Data
    public static class SqlScoreReport {
        private AtomicInteger gt85_ = new AtomicInteger();
        private AtomicInteger gt60_85 = new AtomicInteger();
        private AtomicInteger gt0_60 = new AtomicInteger();
        private AtomicInteger gtF100_0 = new AtomicInteger();
        private AtomicInteger gtF200_F100 = new AtomicInteger();
        private AtomicInteger gtF500_F200 = new AtomicInteger();
        private AtomicInteger gtF1000_F500 = new AtomicInteger();
        private AtomicInteger gtF2000_F1000 = new AtomicInteger();
        private AtomicInteger gtF3000_F2000 = new AtomicInteger();
        private AtomicInteger gtF4000_F3000 = new AtomicInteger();
        private AtomicInteger gtF5000_F4000 = new AtomicInteger();
        private AtomicInteger _gtF5000 = new AtomicInteger();
    }

    @Data
    public static class SqlLengthReport {
        private AtomicInteger gt5000_ = new AtomicInteger();
        private AtomicInteger gt40000_50000 = new AtomicInteger();
        private AtomicInteger gt30000_40000 = new AtomicInteger();
        private AtomicInteger gt20000_30000 = new AtomicInteger();
        private AtomicInteger gt10000_20000 = new AtomicInteger();
        private AtomicInteger gt5000_10000 = new AtomicInteger();
        private AtomicInteger gt2000_5000 = new AtomicInteger();
        private AtomicInteger gt1000_2000 = new AtomicInteger();
        private AtomicInteger gt500_1000 = new AtomicInteger();
        private AtomicInteger _gt500 = new AtomicInteger();
    }

    @Data
    public static class GroupByCountReport {
        private AtomicInteger gt50_ = new AtomicInteger();
        private AtomicInteger gt40_50 = new AtomicInteger();
        private AtomicInteger gt30_40 = new AtomicInteger();
        private AtomicInteger gt20_30 = new AtomicInteger();
        private AtomicInteger gt10_20 = new AtomicInteger();
        private AtomicInteger gt5_10 = new AtomicInteger();
        private AtomicInteger _le5 = new AtomicInteger();
    }

    @Data
    public static class OrderByCountReport {
        private AtomicInteger gt50_ = new AtomicInteger();
        private AtomicInteger gt40_50 = new AtomicInteger();
        private AtomicInteger gt30_40 = new AtomicInteger();
        private AtomicInteger gt20_30 = new AtomicInteger();
        private AtomicInteger gt10_20 = new AtomicInteger();
        private AtomicInteger gt5_10 = new AtomicInteger();
        private AtomicInteger _le5 = new AtomicInteger();
    }

    @Data
    public static class JoinCountReport {
        private AtomicInteger gt50_ = new AtomicInteger();
        private AtomicInteger gt40_50 = new AtomicInteger();
        private AtomicInteger gt30_40 = new AtomicInteger();
        private AtomicInteger gt20_30 = new AtomicInteger();
        private AtomicInteger gt10_20 = new AtomicInteger();
        private AtomicInteger gt5_10 = new AtomicInteger();
        private AtomicInteger _le5 = new AtomicInteger();
    }
}
