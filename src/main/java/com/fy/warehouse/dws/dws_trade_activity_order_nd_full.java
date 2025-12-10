package com.fy.warehouse.dws;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
//活动订单30天汇总表
public class dws_trade_activity_order_nd_full {
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
        tableEnv.useDatabase("dws");

        //创建paimon表
        String paimonSQL = "CREATE TABLE IF NOT EXISTS dws.dws_trade_activity_order_nd_full(\n" +
                "    `activity_id`                BIGINT COMMENT '活动id',\n" +
                "    `k1`                         STRING  COMMENT '分区字段',\n" +
                "    `activity_name`              STRING COMMENT '活动名称',\n" +
                "    `activity_type_code`         STRING COMMENT '活动类型编码',\n" +
                "    `activity_type_name`         STRING COMMENT '活动类型名称',\n" +
                "    `start_date`                 STRING COMMENT '发布日期',\n" +
                "    `original_amount_30d`        DECIMAL(16, 2) COMMENT '参与活动订单原始金额',\n" +
                "    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '参与活动订单优惠金额',\n" +
                "    PRIMARY KEY (`activity_id`,`k1` ) NOT ENFORCED\n" +
                "    )   PARTITIONED BY (`k1` ) WITH (\n" +
                "    'connector' = 'paimon',\n" +
                "    'metastore.partitioned-table' = 'true',\n" +
                "    'file.format' = 'parquet',\n" +
                "    'write-buffer-size' = '512mb',\n" +
                "    'write-buffer-spillable' = 'true' ,\n" +
                "    'partition.expiration-time' = '1 d',\n" +
                "    'partition.expiration-check-interval' = '1 h',\n" +
                "    'partition.timestamp-formatter' = 'yyyy-MM-dd',\n" +
                "    'partition.timestamp-pattern' = '$k1'\n" +
                "    );";

        tableEnv.executeSql(paimonSQL);
        System.out.println("paimon表创建成功");

        //导入
        String insertSQL = "INSERT INTO dws.dws_trade_activity_order_nd_full(activity_id, k1, activity_name, activity_type_code, activity_type_name, start_date, original_amount_30d, activity_reduce_amount_30d)\n" +
                "select\n" +
                "    act.activity_id,\n" +
                "    od.k1,\n" +
                "    activity_name,\n" +
                "    activity_type_code,\n" +
                "    activity_type_name,\n" +
                "    date_format(start_time,'yyyy-MM-dd'),\n" +
                "    sum(split_original_amount),\n" +
                "    sum(split_activity_amount)\n" +
                "from\n" +
                "    (\n" +
                "        select\n" +
                "            activity_id,\n" +
                "            activity_name,\n" +
                "            activity_type_code,\n" +
                "            activity_type_name,\n" +
                "            start_time\n" +
                "        from dim.dim_activity_full\n" +
                "    )act\n" +
                "        left join\n" +
                "    (\n" +
                "        select\n" +
                "            activity_id,\n" +
                "            k1,\n" +
                "            order_id,\n" +
                "            split_original_amount,\n" +
                "            split_activity_amount\n" +
                "        from dwd.dwd_trade_order_detail_full\n" +
                "        where activity_id is not null and timestampdiff(day,to_timestamp(k1,'yyyy-MM-dd'),to_timestamp('2024-05-31','yyyy-MM-dd')) between 0 and 30\n" +
                "    )od\n" +
                "    on act.activity_id=od.activity_id\n" +
                "group by act.activity_id,od.k1,activity_name,activity_type_code,activity_type_name,start_time;";

        tableEnv.executeSql(insertSQL);
        System.out.println("dws_trade_activity_order_nd_full表导入任务启动成功");
    }
}
