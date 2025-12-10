package com.fy.warehouse.ads;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

//活动补贴率表
public class ads_activity_stats_full {
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
        String paimonSQL = "CREATE TABLE IF NOT EXISTS ads.ads_activity_stats_full(\n" +
                "    `dt`            STRING COMMENT '统计日期',\n" +
                "    `activity_id`   BIGINT COMMENT '活动ID',\n" +
                "    `activity_name` STRING COMMENT '活动名称',\n" +
                "    `start_date`    STRING COMMENT '活动开始日期',\n" +
                "    `reduce_rate`   DECIMAL(16, 2) COMMENT '补贴率'\n" +
                "    );";

        tableEnv.executeSql(paimonSQL);

        //导入任务
        String insertSQL = "INSERT INTO ads.ads_activity_stats_full(dt, activity_id, activity_name, start_date, reduce_rate)\n" +
                "select\n" +
                "    k1,\n" +
                "    activity_id,\n" +
                "    activity_name,\n" +
                "    start_date,\n" +
                "    cast(activity_reduce_amount_30d/original_amount_30d as decimal(16,2))\n" +
                "from dws.dws_trade_activity_order_nd_full;";
        tableEnv.executeSql(insertSQL);
        System.out.println("任务启动成功");

    }
}
