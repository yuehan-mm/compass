package com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@Data
public class SqlReport {

    UnionCountReport unionCountReport = new UnionCountReport();
    GroupByCountReport groupByCountReport = new GroupByCountReport();
    JoinCountReport joinCountReport = new JoinCountReport();
    OrderByCountReport orderByCountReport = new OrderByCountReport();

    SqlLengthReport sqlLengthReport = new SqlLengthReport();
    TableRefCountReport tableRefCountReport = new TableRefCountReport();
    TableUseCountReport tableReadCountReport = new TableUseCountReport();

    ScanFileCountReport scanFileCountReport = new ScanFileCountReport();
    ScanFileSizeReport scanFileSizeReport = new ScanFileSizeReport();
    ScanSmallFileCountReport scanSmallFileCountReport = new ScanSmallFileCountReport();
    ScanPartitionCountReport scanPartitionCountReport = new ScanPartitionCountReport();

    @Data
    public static class UnionCountReport {
        StaticElement gt50_ = new StaticElement(">50", new AtomicInteger());
        StaticElement gt40_50 = new StaticElement("40-50", new AtomicInteger());
        StaticElement gt30_40 = new StaticElement("30-40", new AtomicInteger());
        StaticElement gt20_30 = new StaticElement("20-30", new AtomicInteger());
        StaticElement gt10_20 = new StaticElement("10-20", new AtomicInteger());
        StaticElement gt5_10 = new StaticElement("5-10", new AtomicInteger());
        StaticElement _le5 = new StaticElement("<5", new AtomicInteger());
        List list = Arrays.asList(gt50_, gt40_50, gt30_40, gt20_30, gt10_20, gt5_10, _le5);
    }

    @Data
    public static class GroupByCountReport {
        StaticElement gt50_ = new StaticElement(">50", new AtomicInteger());
        StaticElement gt40_50 = new StaticElement("40-50", new AtomicInteger());
        StaticElement gt30_40 = new StaticElement("30-40", new AtomicInteger());
        StaticElement gt20_30 = new StaticElement("20-30", new AtomicInteger());
        StaticElement gt10_20 = new StaticElement("10-20", new AtomicInteger());
        StaticElement gt5_10 = new StaticElement("5-10", new AtomicInteger());
        StaticElement _le5 = new StaticElement("<5", new AtomicInteger());
        List list = Arrays.asList(gt50_, gt40_50, gt30_40, gt20_30, gt10_20, gt5_10, _le5);
    }

    @Data
    public static class JoinCountReport {
        StaticElement gt50_ = new StaticElement(">50", new AtomicInteger());
        StaticElement gt40_50 = new StaticElement("40-50", new AtomicInteger());
        StaticElement gt30_40 = new StaticElement("30-40", new AtomicInteger());
        StaticElement gt20_30 = new StaticElement("20-30", new AtomicInteger());
        StaticElement gt10_20 = new StaticElement("10-20", new AtomicInteger());
        StaticElement gt5_10 = new StaticElement("5-10", new AtomicInteger());
        StaticElement _le5 = new StaticElement("<5", new AtomicInteger());
        List list = Arrays.asList(gt50_, gt40_50, gt30_40, gt20_30, gt10_20, gt5_10, _le5);
    }

    @Data
    public static class OrderByCountReport {
        StaticElement gt50_ = new StaticElement(">50", new AtomicInteger());
        StaticElement gt40_50 = new StaticElement("40-50", new AtomicInteger());
        StaticElement gt30_40 = new StaticElement("30-40", new AtomicInteger());
        StaticElement gt20_30 = new StaticElement("20-30", new AtomicInteger());
        StaticElement gt10_20 = new StaticElement("10-20", new AtomicInteger());
        StaticElement gt5_10 = new StaticElement("5-10", new AtomicInteger());
        StaticElement _le5 = new StaticElement("<5", new AtomicInteger());
        List list = Arrays.asList(gt50_, gt40_50, gt30_40, gt20_30, gt10_20, gt5_10, _le5);
    }

    @Data
    public static class SqlLengthReport {
        StaticElement gt5000_ = new StaticElement(">50000", new AtomicInteger());
        StaticElement gt40000_50000 = new StaticElement("40000-50000", new AtomicInteger());
        StaticElement gt30000_40000 = new StaticElement("30000-40000", new AtomicInteger());
        StaticElement gt20000_30000 = new StaticElement("20000-30000", new AtomicInteger());
        StaticElement gt10000_20000 = new StaticElement("10000-20000", new AtomicInteger());
        StaticElement gt5000_10000 = new StaticElement("5000-10000", new AtomicInteger());
        StaticElement gt2000_5000 = new StaticElement("2000-5000", new AtomicInteger());
        StaticElement gt1000_2000 = new StaticElement("1000-2000", new AtomicInteger());
        StaticElement gt500_1000 = new StaticElement("500-1000", new AtomicInteger());
        StaticElement _gt500 = new StaticElement("<500", new AtomicInteger());
        List list = Arrays.asList(gt5000_, gt40000_50000, gt30000_40000, gt20000_30000, gt10000_20000,
                gt5000_10000, gt2000_5000, gt1000_2000, gt500_1000, _gt500);
    }

    @Data
    public static class TableRefCountReport {
        StaticElement gt50_ = new StaticElement(">50", new AtomicInteger());
        StaticElement gt40_50 = new StaticElement("40-50", new AtomicInteger());
        StaticElement gt30_40 = new StaticElement("30-40", new AtomicInteger());
        StaticElement gt20_30 = new StaticElement("20-30", new AtomicInteger());
        StaticElement gt10_20 = new StaticElement("10-20", new AtomicInteger());
        StaticElement gt5_10 = new StaticElement("5-10", new AtomicInteger());
        StaticElement _le5 = new StaticElement("<5", new AtomicInteger());
        List list = Arrays.asList(gt50_, gt40_50, gt30_40, gt20_30, gt10_20, gt5_10, _le5);
    }

    @Data
    public static class TableUseCountReport {
        StaticElement gt50_ = new StaticElement(">50", new AtomicInteger());
        StaticElement gt40_50 = new StaticElement("40-50", new AtomicInteger());
        StaticElement gt30_40 = new StaticElement("30-40", new AtomicInteger());
        StaticElement gt20_30 = new StaticElement("20-30", new AtomicInteger());
        StaticElement gt10_20 = new StaticElement("10-20", new AtomicInteger());
        StaticElement gt5_10 = new StaticElement("5-10", new AtomicInteger());
        StaticElement _le5 = new StaticElement("<5", new AtomicInteger());
        List list = Arrays.asList(gt50_, gt40_50, gt30_40, gt20_30, gt10_20, gt5_10, _le5);
    }

    @Data
    public static class ScanFileCountReport {
        StaticElement gt50_ = new StaticElement(">50", new AtomicInteger());
        StaticElement gt40_50 = new StaticElement("40-50", new AtomicInteger());
        StaticElement gt30_40 = new StaticElement("30-40", new AtomicInteger());
        StaticElement gt20_30 = new StaticElement("20-30", new AtomicInteger());
        StaticElement gt10_20 = new StaticElement("10-20", new AtomicInteger());
        StaticElement gt5_10 = new StaticElement("5-10", new AtomicInteger());
        StaticElement _le5 = new StaticElement("<5", new AtomicInteger());
        List list = Arrays.asList(gt50_, gt40_50, gt30_40, gt20_30, gt10_20, gt5_10, _le5);
    }

    @Data
    public static class ScanFileSizeReport {
        StaticElement gt50G_ = new StaticElement(">50G", new AtomicInteger());
        StaticElement gt10G_50G = new StaticElement("10G-50G", new AtomicInteger());
        StaticElement gt5G_10G = new StaticElement("5G-10G", new AtomicInteger());
        StaticElement gt1G_5G = new StaticElement("1G-5G", new AtomicInteger());
        StaticElement gt300M_1G = new StaticElement("300M-1G", new AtomicInteger());
        StaticElement gt100M_300M = new StaticElement("100M-300M", new AtomicInteger());
        StaticElement gt50M_100M = new StaticElement("50M-100M", new AtomicInteger());
        StaticElement gt10M_50M = new StaticElement("10M-50M", new AtomicInteger());
        StaticElement _le10M = new StaticElement("<10M", new AtomicInteger());
        List list = Arrays.asList(gt50G_, gt10G_50G, gt5G_10G, gt1G_5G, gt300M_1G, gt100M_300M, gt50M_100M, gt10M_50M, _le10M);
    }

    @Data
    public static class ScanSmallFileCountReport {
        StaticElement gt50_ = new StaticElement(">50", new AtomicInteger());
        StaticElement gt40_50 = new StaticElement("40-50", new AtomicInteger());
        StaticElement gt30_40 = new StaticElement("30-40", new AtomicInteger());
        StaticElement gt20_30 = new StaticElement("20-30", new AtomicInteger());
        StaticElement gt10_20 = new StaticElement("10-20", new AtomicInteger());
        StaticElement gt5_10 = new StaticElement("5-10", new AtomicInteger());
        StaticElement _le5 = new StaticElement("<5", new AtomicInteger());
        List list = Arrays.asList(gt50_, gt40_50, gt30_40, gt20_30, gt10_20, gt5_10, _le5);
    }

    @Data
    public static class ScanPartitionCountReport {
        StaticElement gt50_ = new StaticElement(">50", new AtomicInteger());
        StaticElement gt40_50 = new StaticElement("40-50", new AtomicInteger());
        StaticElement gt30_40 = new StaticElement("30-40", new AtomicInteger());
        StaticElement gt20_30 = new StaticElement("20-30", new AtomicInteger());
        StaticElement gt10_20 = new StaticElement("10-20", new AtomicInteger());
        StaticElement gt5_10 = new StaticElement("5-10", new AtomicInteger());
        StaticElement _le5 = new StaticElement("<5", new AtomicInteger());
        List list = Arrays.asList(gt50_, gt40_50, gt30_40, gt20_30, gt10_20, gt5_10, _le5);
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
    @AllArgsConstructor
    @Getter
    public static class StaticElement {
        private String label;
        private AtomicInteger value;
    }

    public HashMap<String, List<String>> getBi() {
        return new HashMap<String, List<String>>() {{
            put("unionCountReport", unionCountReport.getList());
            put("groupByCountReport", groupByCountReport.getList());
            put("joinCountReport", joinCountReport.getList());
            put("orderByCountReport", orderByCountReport.getList());
            put("sqlLengthReport", sqlLengthReport.getList());
            put("tableRefCountReport", tableRefCountReport.getList());
            put("tableReadCountReport", tableReadCountReport.getList());
        }};
    }
}
