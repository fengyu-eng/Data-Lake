package com.fy.warehouse.ODS;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.concurrent.ExecutionException;

// 类名对应购物车表：ods_cart_info_full
public class ods_cart_info_full {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Configuration config = FlinkConfigUtil.getFlinkConfig();

        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance()
                .withConfiguration(config)
                .inStreamingMode()
                .build());

        //创建mysql-cdc映射表（复用统一集群地址）
        String cdcSql = "CREATE TABLE cart_info_full_mq (\n" +
                "    `id` bigint NOT NULL COMMENT '编号',\n" +
                "    `user_id` STRING NULL COMMENT '用户id',\n" +
                "    `sku_id` bigint NULL COMMENT 'skuid',\n" +
                "    `cart_price` decimal(10,2) NULL COMMENT '放入购物车时价格',\n" +
                "    `sku_num` int NULL COMMENT '数量',\n" +
                "    `img_url` STRING NULL COMMENT '图片文件',\n" +
                "    `sku_name` STRING NULL COMMENT 'sku名称 (冗余)',\n" +
                "    `is_checked` int NULL,\n" +
                "    `create_time` TIMESTAMP(3) NULL COMMENT '创建时间',\n" +
                "    `operate_time` TIMESTAMP(3) NULL COMMENT '修改时间',\n" +
                "    `is_ordered` bigint NULL COMMENT '是否已经下单',\n" +
                "    `order_time` TIMESTAMP(3) NULL COMMENT '下单时间',\n" +
                "    `source_type` STRING NULL COMMENT '来源类型',\n" +
                "    `source_id` bigint NULL COMMENT '来源编号',\n" +
                "    PRIMARY KEY(`id`) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'mysql-cdc',\n" +
                "    'scan.startup.mode' = 'initial',\n" + // 复用统一启动模式
                "    'hostname' = '192.168.10.102',\n" + // 统一集群MySQL地址
                "    'port' = '3306',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '123456',\n" + // 统一MySQL密码
                "    'database-name' = 'gmall',\n" +
                "    'table-name' = 'cart_info',\n" + // 购物车表名
                "    'server-time-zone' = 'Asia/Shanghai'\n" +
                ");";
        tableEnv.executeSql(cdcSql);
        System.out.println("MySql CDC表创建成功");

        //创建catalog（复用统一集群地址）
        String catalogSql = "CREATE CATALOG paimon_hive WITH (\n" +
                "    'type' = 'paimon',\n" +
                "    'metastore' = 'hive',\n" +
                "    'uri' = 'thrift://192.168.10.102:9083',\n" + // 统一Hive URI
                "    'warehouse' = 'hdfs://192.168.10.102/user/hive/warehouse'\n" + // 统一HDFS路径
                ");";

        tableEnv.executeSql(catalogSql);
        System.out.println("创建catalog成功");

        tableEnv.useCatalog("paimon_hive");
        tableEnv.useDatabase("ods"); // 复用统一数据库切换逻辑
        System.out.println("切换到ods库，paimon_hive Catalog");

        //创建paimon表（完整保留购物车表分区配置和所有字段）
        String paimonDDl = "CREATE TABLE IF NOT EXISTS ods.ods_cart_info_full(\n" +
                "    `id` bigint NOT NULL COMMENT '编号',\n" +
                "    `k1` STRING COMMENT '分区字段',\n" +
                "    `user_id` STRING NULL COMMENT '用户id',\n" +
                "    `sku_id` bigint NULL COMMENT 'skuid',\n" +
                "    `cart_price` decimal(10,2) NULL COMMENT '放入购物车时价格',\n" +
                "    `sku_num` int NULL COMMENT '数量',\n" +
                "    `img_url` STRING NULL COMMENT '图片文件',\n" +
                "    `sku_name` STRING NULL COMMENT 'sku名称 (冗余)',\n" +
                "    `is_checked` int NULL,\n" +
                "    `create_time` TIMESTAMP(3) NULL COMMENT '创建时间',\n" +
                "    `operate_time` TIMESTAMP(3) NULL COMMENT '修改时间',\n" +
                "    `is_ordered` bigint NULL COMMENT '是否已经下单',\n" +
                "    `order_time` TIMESTAMP(3) NULL COMMENT '下单时间',\n" +
                "    `source_type` STRING NULL COMMENT '来源类型',\n" +
                "    `source_id` bigint NULL COMMENT '来源编号',\n" +
                "    PRIMARY KEY (`id`,`k1`) NOT ENFORCED\n" + // 复合主键（含分区字段）
                ") PARTITIONED BY (`k1`) WITH (\n" +
                "    'connector' = 'paimon',\n" +
                "    'metastore.partitioned-table' = 'true',\n" +
                "    'file.format' = 'parquet',\n" +
                "    'write-buffer-size' = '512mb',\n" +
                "    'write-buffer-spillable' = 'true',\n" +
                "    'partition.expiration-time' = '1 d',\n" +
                "    'partition.expiration-check-interval' = '1 h',\n" +
                "    'partition.timestamp-formatter' = 'yyyy-MM-dd',\n" +
                "    'partition.timestamp-pattern' = '$k1'\n" +
                ");";
        tableEnv.executeSql(paimonDDl);
        System.out.println("paimon表创建成功");

        //导入任务（完整保留分区逻辑、所有字段同步和过滤条件）
        String insertSql = "INSERT INTO ods.ods_cart_info_full(\n" +
                "    `id`,\n" +
                "    `k1`,\n" +
                "    `user_id`,\n" +
                "    `sku_id`,\n" +
                "    `cart_price`,\n" +
                "    `sku_num`,\n" +
                "    `img_url`,\n" +
                "    `sku_name`,\n" +
                "    `is_checked`,\n" +
                "    `create_time`,\n" +
                "    `operate_time`,\n" +
                "    `is_ordered`,\n" +
                "    `order_time`,\n" +
                "    `source_type`,\n" +
                "    `source_id`\n" +
                ")\n" +
                "select\n" +
                "    `id`,\n" +
                "    DATE_FORMAT(create_time, 'yyyy-MM-dd') AS k1,\n" + // 分区字段：创建时间按天分区
                "    `user_id`,\n" +
                "    `sku_id`,\n" +
                "    `cart_price`,\n" +
                "    `sku_num`,\n" +
                "    `img_url`,\n" +
                "    `sku_name`,\n" +
                "    `is_checked`,\n" +
                "    `create_time`,\n" +
                "    `operate_time`,\n" +
                "    `is_ordered`,\n" +
                "    `order_time`,\n" +
                "    `source_type`,\n" +
                "    `source_id`\n" +
                "from default_catalog.default_database.cart_info_full_mq\n" +
                "where create_time is not null;"; // 保留原SQL的过滤条件：排除create_time为空的数据

        TableResult tableResult = tableEnv.executeSql(insertSql);
        System.out.println("cart_info导入任务启动成功");
        tableResult.await();
    }
}