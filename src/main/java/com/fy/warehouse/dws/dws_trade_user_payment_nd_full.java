package com.fy.warehouse.dws;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

// 交易域用户粒度支付最近30日汇总表
public class dws_trade_user_payment_nd_full {
    public static void main(String[] args) {
        // 获取Flink配置
        Configuration config = FlinkConfigUtil.getFlinkConfig();
        // 创建TableEnvironment
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance()
                .withConfiguration(config)
                .build());

        // 创建catalog
        String catalogSql = "CREATE CATALOG paimon_hive WITH (\n" +
                "    'type' = 'paimon',\n" +
                "    'metastore' = 'hive',\n" +
                "    'uri' = 'thrift://192.168.10.102:9083',\n" +
                "    'warehouse' = 'hdfs://192.168.10.102/user/hive/warehouse'\n" +
                ");";
        tableEnv.executeSql(catalogSql);
        System.out.println("catalog创建成功");

        // 使用指定的catalog和database
        tableEnv.useCatalog("paimon_hive");
        tableEnv.executeSql("create DATABASE IF NOT EXISTS dws;");
        tableEnv.useDatabase("dws");

        // 创建paimon表
        String paimonSql = "CREATE TABLE IF NOT EXISTS dws.dws_trade_user_payment_nd_full(\n" +
                "    `user_id`            BIGINT COMMENT '用户id',\n" +
                "    `k1`                 STRING COMMENT '分区字段',\n" +
                "    `payment_count_30d`  BIGINT COMMENT '最近30日支付次数',\n" +
                "    `payment_num_30d`    BIGINT COMMENT '最近30日支付商品件数',\n" +
                "    `payment_amount_30d` DECIMAL(16, 2) COMMENT '最近30日支付金额',\n" +
                "    PRIMARY KEY (`user_id`,`k1` ) NOT ENFORCED\n" +
                ")   PARTITIONED BY (`k1` ) WITH (\n" +
                "    'connector' = 'paimon',\n" +
                "    'metastore.partitioned-table' = 'true',\n" +
                "    'file.format' = 'parquet',\n" +
                "    'write-buffer-size' = '512mb',\n" +
                "    'write-buffer-spillable' = 'true' ,\n" +
                "    'partition.expiration-time' = '1 d',\n" +
                "    'partition.expiration-check-interval' = '1 h',\n" +
                "    'partition.timestamp-formatter' = 'yyyy-MM-dd',\n" +
                "    'partition.timestamp-pattern' = '$k1'\n" +
                ");";

        tableEnv.executeSql(paimonSql);
        System.out.println("paimon表创建成功");

        // 数据插入
        String insertSql = "insert into dws_trade_user_payment_nd_full select\n" +
                "    `user_id`            ,--BIGINT COMMENT '用户id',\n" +
                "    `k1`                 ,--STRING COMMENT '分区字段',\n" +
                "    count(order_id)`payment_count_30d`  ,--BIGINT COMMENT '最近30日支付次数',\n" +
                "    sum(sku_num)`payment_num_30d`    ,--BIGINT COMMENT '最近30日支付商品件数',\n" +
                "    sum(split_payment_amount)`payment_amount_30d` --DIMAL(16, 2) COMMENT '最近30日支付金额'\n" +
                "from (\n" +
                "    select\n" +
                "        user_id,\n" +
                "        k1,\n" +
                "        order_id,\n" +
                "        sku_num,\n" +
                "        split_payment_amount\n" +
                "    from dwd.dwd_trade_pay_detail_suc_full\n" +
                "    where timestampdiff(day,to_timestamp(k1,'yyyy-MM-dd'),to_timestamp('2024-05-31','yyyy-MM-dd')) between 0 and 30\n" +
                "     ) t\n" +
                "group by user_id,k1";

        tableEnv.executeSql(insertSql);
        System.out.println("dws_trade_user_payment_nd_full导入任务启动成功");
    }
}