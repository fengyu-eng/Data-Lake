package com.fy.warehouse.ODS;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.concurrent.ExecutionException;

// 类名对应退款表：ods_refund_payment_full
public class ods_refund_payment_full {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Configuration config = FlinkConfigUtil.getFlinkConfig();

        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance()
                .withConfiguration(config)
                .inStreamingMode()
                .build());

        // 创建mysql-cdc映射表（复用统一集群地址，修正id字段类型）
        String cdcSql = "CREATE TABLE refund_payment_full_mq (\n" +
                "    `id` int NOT NULL COMMENT '编号',\n" + // 保留源表int类型
                "    `out_trade_no` STRING NULL COMMENT '对外业务编号',\n" +
                "    `order_id` bigint NULL COMMENT '订单编号',\n" +
                "    `sku_id` bigint NULL COMMENT 'skuid',\n" +
                "    `payment_type` STRING NULL COMMENT '支付类型（微信 支付宝）',\n" +
                "    `trade_no` STRING NULL COMMENT '交易编号',\n" +
                "    `total_amount` decimal(10,2) NULL COMMENT '退款金额',\n" +
                "    `subject` STRING NULL COMMENT '交易内容',\n" +
                "    `refund_status` STRING NULL COMMENT '退款状态',\n" +
                "    `create_time` timestamp(3) NOT NULL COMMENT '创建时间',\n" +
                "    `callback_time` timestamp(3) NULL COMMENT '回调时间',\n" +
                "    `callback_content` STRING COMMENT '回调信息',\n" +
                "    PRIMARY KEY(`id`) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'mysql-cdc',\n" +
                "    'scan.startup.mode' = 'initial',\n" + // 统一启动模式
                "    'hostname' = '192.168.10.102',\n" + // 统一MySQL地址
                "    'port' = '3306',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '123456',\n" + // 统一MySQL密码
                "    'database-name' = 'gmall',\n" +
                "    'table-name' = 'refund_payment',\n" + // 源表名
                "    'server-time-zone' = 'Asia/Shanghai'\n" +
                ");";
        tableEnv.executeSql(cdcSql);
        System.out.println("MySql CDC表创建成功");

        // 创建catalog（复用统一集群地址）
        String catalogSql = "CREATE CATALOG paimon_hive WITH (\n" +
                "    'type' = 'paimon',\n" +
                "    'metastore' = 'hive',\n" +
                "    'uri' = 'thrift://192.168.10.102:9083',\n" + // 统一Hive URI
                "    'warehouse' = 'hdfs://192.168.10.102/user/hive/warehouse'\n" + // 统一HDFS路径
                ");";
        tableEnv.executeSql(catalogSql);
        System.out.println("创建catalog成功");

        tableEnv.useCatalog("paimon_hive");
        tableEnv.useDatabase("ods"); // 统一数据库切换逻辑
        System.out.println("切换到ods库，paimon_hive Catalog");

        // 创建paimon表（完整保留分区配置，修正id字段类型和注释错误）
        String paimonDDl = "CREATE TABLE IF NOT EXISTS ods.ods_refund_payment_full(\n" +
                "    `id` int NOT NULL COMMENT '编号',\n" + // 修正：原SQL误写为bigint，还原源表int类型；修正注释（原为"购物券编号"）
                "    `k1` STRING COMMENT '分区字段',\n" +
                "    `out_trade_no` STRING NULL COMMENT '对外业务编号',\n" +
                "    `order_id` bigint NULL COMMENT '订单编号',\n" +
                "    `sku_id` bigint NULL COMMENT 'skuid',\n" +
                "    `payment_type` STRING NULL COMMENT '支付类型（微信 支付宝）',\n" +
                "    `trade_no` STRING NULL COMMENT '交易编号',\n" +
                "    `total_amount` decimal(10,2) NULL COMMENT '退款金额',\n" +
                "    `subject` STRING NULL COMMENT '交易内容',\n" +
                "    `refund_status` STRING NULL COMMENT '退款状态',\n" +
                "    `create_time` timestamp(3) NOT NULL COMMENT '创建时间',\n" +
                "    `callback_time` timestamp(3) NULL COMMENT '回调时间',\n" +
                "    `callback_content` STRING COMMENT '回调信息',\n" +
                "    PRIMARY KEY (`id`,`k1`) NOT ENFORCED\n" + // 复合主键（id为int类型不影响唯一性）
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

        // 导入任务（完整保留分区逻辑和过滤条件）
        String insertSql = "INSERT INTO ods.ods_refund_payment_full(\n" +
                "    `id`, `k1`, `out_trade_no`, `order_id`, `sku_id`,\n" +
                "    `payment_type`, `trade_no`, `total_amount`, `subject`,\n" +
                "    `refund_status`, `create_time`, `callback_time`, `callback_content`\n" +
                ")\n" +
                "select\n" +
                "    id,\n" +
                "    DATE_FORMAT(create_time, 'yyyy-MM-dd') AS k1,\n" + // 按创建时间天分区
                "    `out_trade_no`, `order_id`, `sku_id`,\n" +
                "    `payment_type`, `trade_no`, `total_amount`, `subject`,\n" +
                "    `refund_status`, `create_time`, `callback_time`, `callback_content`\n" +
                "from default_catalog.default_database.refund_payment_full_mq\n" +
                "where create_time is not null;"; // 保留过滤条件
        TableResult tableResult = tableEnv.executeSql(insertSql);
        System.out.println("refund_payment导入任务启动成功");
        tableResult.await();
    }
}