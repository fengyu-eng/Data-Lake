package com.fy.warehouse.dws;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import com.fy.warehouse.config.FlinkConfigUtil;

// 交易域用户SKU粒度订单30日汇总表
public class dws_trade_user_sku_order_refund_nd_full {
    public static void main(String[] args) {
        // 获取Flink配置
        Configuration config = FlinkConfigUtil.getFlinkConfig();

        // 设置Flink Table环境参数
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .withConfiguration(config)
                .build();

        // 创建TableEnvironment
        TableEnvironment tableEnv = TableEnvironment.create(settings);


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
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS dws;");
        tableEnv.useDatabase("dws");

        // 创建paimon表
        String createTableSql = "CREATE TABLE IF NOT EXISTS dws.dws_trade_user_sku_order_refund_nd_full(\n" +
                "    `user_id`                     BIGINT COMMENT '用户id',\n" +
                "    `sku_id`                      BIGINT COMMENT 'sku_id',\n" +
                "    `k1`                          STRING COMMENT '分区字段',\n" +
                "    `sku_name`                    STRING COMMENT 'sku名称',\n" +
                "    `category1_id`                BIGINT COMMENT '一级分类id',\n" +
                "    `category1_name`              STRING COMMENT '一级分类名称',\n" +
                "    `category2_id`                BIGINT COMMENT '一级分类id',\n" +
                "    `category2_name`              STRING COMMENT '一级分类名称',\n" +
                "    `category3_id`                BIGINT COMMENT '一级分类id',\n" +
                "    `category3_name`              STRING COMMENT '一级分类名称',\n" +
                "    `tm_id`                       BIGINT COMMENT '品牌id',\n" +
                "    `tm_name`                     STRING COMMENT '品牌名称',\n" +
                "    `order_refund_count_30d`      BIGINT COMMENT '最近30日退单次数',\n" +
                "    `order_refund_num_30d`        BIGINT COMMENT '最近30日退单件数',\n" +
                "    `order_refund_amount_30d`     DECIMAL(16, 2) COMMENT '最近30日退单金额',\n" +
                "    PRIMARY KEY (`user_id`,`sku_id`,`k1` ) NOT ENFORCED\n" +
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

        tableEnv.executeSql(createTableSql);
        System.out.println("dws_trade_user_sku_order_refund_nd_full表创建成功");

        // 数据插入
        String insertSql = "INSERT INTO dws.dws_trade_user_sku_order_refund_nd_full " +
                "SELECT " +
                "    t1.user_id, " +                  // 用户id
                "    t1.sku_id, " +                   // sku_id
                "    t1.k1, " +                       // 分区字段
                "    t2.sku_name, " +                 // sku名称
                "    t2.category1_id, " +             // 一级分类id
                "    t2.category1_name, " +           // 一级分类名称
                "    t2.category2_id, " +             // 二级分类id
                "    t2.category2_name, " +           // 二级分类名称
                "    t2.category3_id, " +             // 三级分类id
                "    t2.category3_name, " +           // 三级分类名称
                "    t2.tm_id, " +                    // 品牌id
                "    t2.tm_name, " +                  // 品牌名称
                "    COUNT(t1.sku_id) AS order_refund_count_30d, " +  // 最近30日退单次数
                "    SUM(COALESCE(t3.refund_num, 0)) AS order_refund_num_30d, " +  // 最近30日退单件数
                "    SUM(COALESCE(t3.refund_amount, 0)) AS order_refund_amount_30d " +  // 最近30日退单金额
                "FROM (" +
                "    SELECT " +
                "        user_id, " +
                "        sku_id, " +
                "        order_id, " +
                "        k1 " +
                "    FROM dwd.dwd_trade_order_detail_full " +
                "    WHERE TIMESTAMPDIFF(DAY, TO_TIMESTAMP(k1,'yyyy-MM-dd'), TO_TIMESTAMP('2024-05-31','yyyy-MM-dd')) BETWEEN 0 AND 30 " +
                ") t1 " +
                "LEFT JOIN (" +
                "    SELECT " +
                "        id, " +
                "        sku_name, " +
                "        category1_id, " +
                "        category1_name, " +
                "        category2_id, " +
                "        category2_name, " +
                "        category3_id, " +
                "        category3_name, " +
                "        tm_id, " +
                "        tm_name " +
                "    FROM dim.dim_sku_full " +
                ") t2 ON t1.sku_id = t2.id " +
                "LEFT JOIN (" +
                "    SELECT " +
                "        sku_id, " +
                "        order_id, " +
                "        refund_num, " +
                "        refund_amount " +
                "    FROM dwd.dwd_trade_order_refund_full " +
                "    WHERE TIMESTAMPDIFF(DAY, TO_TIMESTAMP(k1,'yyyy-MM-dd'), TO_TIMESTAMP('2024-05-31','yyyy-MM-dd')) BETWEEN 0 AND 30 " +
                ") t3 ON t1.order_id = t3.order_id and t1.sku_id = t3.sku_id " +
                "GROUP BY " +
                "    t1.user_id, " +
                "    t1.sku_id, " +
                "    t1.k1, " +
                "    t2.sku_name, " +
                "    t2.category1_id, " +
                "    t2.category1_name, " +
                "    t2.category2_id, " +
                "    t2.category2_name, " +
                "    t2.category3_id, " +
                "    t2.category3_name, " +
                "    t2.tm_id, " +
                "    t2.tm_name";

        tableEnv.executeSql(insertSql);
        System.out.println("dws_trade_user_sku_order_refund_nd数据导入任务启动成功");
    }
}