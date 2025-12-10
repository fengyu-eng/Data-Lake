package com.fy.warehouse.ads;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class ads_coupon_stats_full {
    public static void main(String[] args) {
        Configuration config = FlinkConfigUtil.getFlinkConfig();
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance()
                .withConfiguration(config)
                .build());

        //创建catalog
        String catalogSQL = "CREATE CATALOG paimon_hive WITH (\n" +
                "    'type' = 'paimon',\n" +
                "    'metastore' = 'hive',\n" +
                "    'uri' = 'thrift://192.168.10.102:9083',\n" +
                "    'warehouse' = 'hdfs://192.168.10.102/user/hive/warehouse'\n" +
                ");";

        tableEnv.executeSql(catalogSQL);
        System.out.println("catalog创建成功");

        tableEnv.useCatalog("paimon_hive");
        tableEnv.useDatabase("ads");

        //创建paimon表
        String paimonSQL = "CREATE TABLE IF NOT EXISTS ads.ads_coupon_stats_full(\n" +
                "    `dt`          STRING COMMENT '统计日期',\n" +
                "    `coupon_id`   BIGINT COMMENT '优惠券ID',\n" +
                "    `coupon_name` STRING COMMENT '优惠券名称',\n" +
                "    `start_date`  STRING COMMENT '发布日期',\n" +
                "    `rule_name`   STRING COMMENT '优惠规则，例如满100元减10元',\n" +
                "    `reduce_rate` DECIMAL(16, 2) COMMENT '补贴率'\n" +
                "    );";

        tableEnv.executeSql(paimonSQL);
        System.out.println("paimon表创建成功");

        //导入
        String insertSQL = "INSERT INTO ads.ads_coupon_stats_full(\n" +
                "    dt,\n" +
                "    coupon_id,\n" +
                "    coupon_name,\n" +
                "    start_date,\n" +
                "    rule_name,\n" +
                "    reduce_rate\n" +
                "    )\n" +
                "select\n" +
                "    k1,\n" +
                "    coupon_id,\n" +
                "    coupon_name,\n" +
                "    start_date,\n" +
                "    coupon_rule,\n" +
                "    cast(coupon_reduce_amount_30d/original_amount_30d as decimal(16,2))\n" +
                "from dws.dws_trade_coupon_order_nd_full;";
        tableEnv.executeSql(insertSQL);
        System.out.println("任务启动成功");
    }
}
