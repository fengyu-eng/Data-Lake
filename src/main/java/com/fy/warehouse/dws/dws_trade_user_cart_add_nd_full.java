package com.fy.warehouse.dws;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

// 交易域用户粒度加购最近30日汇总表
public class dws_trade_user_cart_add_nd_full {
    public static void main(String[] args) {
        // 获取Flink配置
        Configuration config = FlinkConfigUtil.getFlinkConfig();
        // 创建TableEnvironment
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance()
                .withConfiguration(config)
                .build());

        // 创建catalog（URL沿用Java代码的192.168.10.102:9083）
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
        String paimonSql = "CREATE TABLE IF NOT EXISTS dws.dws_trade_user_cart_add_nd_full(\n" +
                "    `user_id`                STRING COMMENT '用户id',\n" +
                "    `k1`                     STRING COMMENT '分区字段',\n" +
                "    `cart_add_count_nd`      BIGINT COMMENT '最近30日加购次数',\n" +
                "    `cart_add_num_nd`        BIGINT COMMENT '最近30日加购商品件数',\n" +
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
        String insertSql = "insert into dws.dws_trade_user_cart_add_nd_full select\n" +
                "    `user_id`            ,--       STRING COMMENT '用户id',\n" +
                "    `k1`                 ,--       STRING  COMMENT '分区字段',\n" +
                "    count(sku_id)`cart_add_count_30d` ,--BIGINT COMMENT '最近30日加购次数',\n" +
                "    sum(sku_num) `cart_add_num_30d`   --BIGINT COMMENT '最近30日加购商品件数'\n" +
                "from (\n" +
                "    select\n" +
                "        user_id,\n" +
                "        k1,\n" +
                "        sku_id,\n" +
                "        sku_num\n" +
                "    from dwd.dwd_trade_cart_full\n" +
                "    where timestampdiff(day,to_timestamp(k1,'yyyy-MM-dd'),to_timestamp('2024-05-31','yyyy-MM-dd')) between 0 and 30\n" +
                "     ) t\n" +
                "group by user_id,k1;";

        tableEnv.executeSql(insertSql);
        System.out.println("dws_trade_user_cart_add_1d_full导入任务启动成功");
    }
}