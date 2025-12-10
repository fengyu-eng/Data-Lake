package com.fy.warehouse.ODS;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.concurrent.ExecutionException;

// 类名对应优惠券使用表：ods_coupon_use_full
public class ods_coupon_use_full {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Configuration config = FlinkConfigUtil.getFlinkConfig();

        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance()
                .withConfiguration(config)
                .inStreamingMode()
                .build());

        //创建mysql-cdc映射表（复用统一集群地址）
        String cdcSql = "CREATE TABLE coupon_use_full_mq (\n" +
                "    `id` bigint NOT NULL COMMENT '编号',\n" +
                "    `coupon_id` bigint NULL COMMENT '购物券ID',\n" +
                "    `user_id` bigint NULL COMMENT '用户ID',\n" +
                "    `order_id` bigint NULL COMMENT '订单ID',\n" +
                "    `coupon_status` STRING NULL COMMENT '购物券状态（1：未使用 2：已使用）',\n" +
                "    `create_time` timestamp(3) NOT NULL COMMENT '创建时间',\n" +
                "    `get_time` timestamp(3) NULL COMMENT '获取时间',\n" +
                "    `using_time` timestamp(3) NULL COMMENT '使用时间',\n" +
                "    `used_time` timestamp(3) NULL COMMENT '支付时间',\n" +
                "    `expire_time` timestamp(3) NULL COMMENT '过期时间',\n" +
                "    PRIMARY KEY(`id`) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'mysql-cdc',\n" +
                "    'scan.startup.mode' = 'initial',\n" + // 复用统一启动模式
                "    'hostname' = '192.168.10.102',\n" + // 统一集群MySQL地址
                "    'port' = '3306',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '123456',\n" + // 统一MySQL密码
                "    'database-name' = 'gmall',\n" +
                "    'table-name' = 'coupon_use',\n" + // 优惠券使用表名
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

        //创建paimon表（完整保留分区配置和所有字段，修正原SQL注释错误）
        String paimonDDl = "CREATE TABLE IF NOT EXISTS ods.ods_coupon_use_full(\n" +
                "    `id` bigint NOT NULL COMMENT '编号',\n" + // 修正原SQL注释（原为"购物券编号"）
                "    `k1` STRING COMMENT '分区字段',\n" +
                "    `coupon_id` bigint NULL COMMENT '购物券ID',\n" +
                "    `user_id` bigint NULL COMMENT '用户ID',\n" +
                "    `order_id` bigint NULL COMMENT '订单ID',\n" +
                "    `coupon_status` STRING NULL COMMENT '购物券状态（1：未使用 2：已使用）',\n" +
                "    `create_time` timestamp(3) NOT NULL COMMENT '创建时间',\n" +
                "    `get_time` timestamp(3) NULL COMMENT '获取时间',\n" +
                "    `using_time` timestamp(3) NULL COMMENT '使用时间',\n" +
                "    `used_time` timestamp(3) NULL COMMENT '支付时间',\n" +
                "    `expire_time` timestamp(3) NULL COMMENT '过期时间',\n" +
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
        String insertSql = "INSERT INTO ods.ods_coupon_use_full(\n" +
                "    `id`,\n" +
                "    `k1`,\n" +
                "    `coupon_id`,\n" +
                "    `user_id`,\n" +
                "    `order_id`,\n" +
                "    `coupon_status`,\n" +
                "    `create_time`,\n" +
                "    `get_time`,\n" +
                "    `using_time`,\n" +
                "    `used_time`,\n" +
                "    `expire_time`\n" +
                ")\n" +
                "select\n" +
                "    id,\n" +
                "    DATE_FORMAT(create_time, 'yyyy-MM-dd') AS k1,\n" + // 按创建时间天分区
                "    `coupon_id`,\n" +
                "    `user_id`,\n" +
                "    `order_id`,\n" +
                "    `coupon_status`,\n" +
                "    `create_time`,\n" +
                "    `get_time`,\n" +
                "    `using_time`,\n" +
                "    `used_time`,\n" +
                "    `expire_time`\n" +
                "from default_catalog.default_database.coupon_use_full_mq\n" +
                "where create_time is not null;"; // 保留过滤条件

        TableResult tableResult = tableEnv.executeSql(insertSql);
        System.out.println("coupon_use导入任务启动成功");
        tableResult.await();
    }
}