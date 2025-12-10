package com.fy.warehouse.ODS;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.concurrent.ExecutionException;

// 类名对应订单表：ods_order_info_full
public class ods_order_info_full {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Configuration config = FlinkConfigUtil.getFlinkConfig();

        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance()
                .withConfiguration(config)
                .inStreamingMode()
                .build());

        // 创建mysql-cdc映射表（复用统一集群地址）
        String cdcSql = "CREATE TABLE order_info_full_mq (\n" +
                "    `id` bigint NOT NULL COMMENT '编号',\n" +
                "    `consignee` STRING NULL COMMENT '收货人',\n" +
                "    `consignee_tel` STRING NULL COMMENT '收件人电话',\n" +
                "    `total_amount` decimal(10,2) NULL COMMENT '总金额',\n" +
                "    `order_status` STRING NULL COMMENT '订单状态',\n" +
                "    `user_id` bigint NULL COMMENT '用户id',\n" +
                "    `payment_way` STRING NULL COMMENT '付款方式',\n" +
                "    `delivery_address` STRING NULL COMMENT '送货地址',\n" +
                "    `order_comment` STRING NULL COMMENT '订单备注',\n" +
                "    `out_trade_no` STRING NULL COMMENT '订单交易编号（第三方支付用)',\n" +
                "    `trade_body` STRING NULL COMMENT '订单描述(第三方支付用)',\n" +
                "    `create_time` timestamp(3) NOT NULL COMMENT '创建时间',\n" +
                "    `operate_time` timestamp(3) NOT NULL COMMENT '操作时间',\n" +
                "    `expire_time` timestamp(3) NULL COMMENT '失效时间',\n" +
                "    `process_status` STRING NULL COMMENT '进度状态',\n" +
                "    `tracking_no` STRING NULL COMMENT '物流单编号',\n" +
                "    `parent_order_id` bigint NULL COMMENT '父订单编号',\n" +
                "    `img_url` STRING NULL COMMENT '图片路径',\n" +
                "    `province_id` int NULL COMMENT '地区',\n" +
                "    `activity_reduce_amount` decimal(16,2) NULL COMMENT '促销金额',\n" +
                "    `coupon_reduce_amount` decimal(16,2) NULL COMMENT '优惠券',\n" +
                "    `original_total_amount` decimal(16,2) NULL COMMENT '原价金额',\n" +
                "    `feight_fee` decimal(16,2) NULL COMMENT '运费',\n" +
                "    `feight_fee_reduce` decimal(16,2) NULL COMMENT '运费减免',\n" +
                "    `refundable_time` timestamp(3) NULL COMMENT '可退款日期（签收后30天）',\n" +
                "    PRIMARY KEY(`id`) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'mysql-cdc',\n" +
                "    'scan.startup.mode' = 'initial',\n" + // 统一启动模式
                "    'hostname' = '192.168.10.102',\n" + // 统一MySQL地址
                "    'port' = '3306',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '123456',\n" + // 统一MySQL密码
                "    'database-name' = 'gmall',\n" +
                "    'table-name' = 'order_info',\n" + // 源表名
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

        // 创建paimon表（完整保留分区配置，修正id字段注释错误）
        String paimonDDl = "CREATE TABLE IF NOT EXISTS ods.ods_order_info_full(\n" +
                "    `id` bigint NOT NULL COMMENT '编号',\n" + // 修正原注释（原为"购物券编号"）
                "    `k1` STRING COMMENT '分区字段',\n" +
                "    `consignee` STRING NULL COMMENT '收货人',\n" +
                "    `consignee_tel` STRING NULL COMMENT '收件人电话',\n" +
                "    `total_amount` decimal(10,2) NULL COMMENT '总金额',\n" +
                "    `order_status` STRING NULL COMMENT '订单状态',\n" +
                "    `user_id` bigint NULL COMMENT '用户id',\n" +
                "    `payment_way` STRING NULL COMMENT '付款方式',\n" +
                "    `delivery_address` STRING NULL COMMENT '送货地址',\n" +
                "    `order_comment` STRING NULL COMMENT '订单备注',\n" +
                "    `out_trade_no` STRING NULL COMMENT '订单交易编号（第三方支付用)',\n" +
                "    `trade_body` STRING NULL COMMENT '订单描述(第三方支付用)',\n" +
                "    `create_time` timestamp(3) NOT NULL COMMENT '创建时间',\n" +
                "    `operate_time` timestamp(3) NOT NULL COMMENT '操作时间',\n" +
                "    `expire_time` timestamp(3) NULL COMMENT '失效时间',\n" +
                "    `process_status` STRING NULL COMMENT '进度状态',\n" +
                "    `tracking_no` STRING NULL COMMENT '物流单编号',\n" +
                "    `parent_order_id` bigint NULL COMMENT '父订单编号',\n" +
                "    `img_url` STRING NULL COMMENT '图片路径',\n" +
                "    `province_id` int NULL COMMENT '地区',\n" +
                "    `activity_reduce_amount` decimal(16,2) NULL COMMENT '促销金额',\n" +
                "    `coupon_reduce_amount` decimal(16,2) NULL COMMENT '优惠券',\n" +
                "    `original_total_amount` decimal(16,2) NULL COMMENT '原价金额',\n" +
                "    `feight_fee` decimal(16,2) NULL COMMENT '运费',\n" +
                "    `feight_fee_reduce` decimal(16,2) NULL COMMENT '运费减免',\n" +
                "    `refundable_time` timestamp(3) NULL COMMENT '可退款日期（签收后30天）',\n" +
                "    PRIMARY KEY (`id`,`k1`) NOT ENFORCED\n" + // 复合主键
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
        String insertSql = "INSERT INTO ods.ods_order_info_full(\n" +
                "    `id`, `k1`, `consignee`, `consignee_tel`, `total_amount`, `order_status`,\n" +
                "    `user_id`, `payment_way`, `delivery_address`, `order_comment`, `out_trade_no`,\n" +
                "    `trade_body`, `create_time`, `operate_time`, `expire_time`, `process_status`,\n" +
                "    `tracking_no`, `parent_order_id`, `img_url`, `province_id`, `activity_reduce_amount`,\n" +
                "    `coupon_reduce_amount`, `original_total_amount`, `feight_fee`, `feight_fee_reduce`,\n" +
                "    `refundable_time`\n" +
                ")\n" +
                "select\n" +
                "    id,\n" +
                "    DATE_FORMAT(create_time, 'yyyy-MM-dd') AS k1,\n" + // 按创建时间天分区
                "    `consignee`, `consignee_tel`, `total_amount`, `order_status`,\n" +
                "    `user_id`, `payment_way`, `delivery_address`, `order_comment`, `out_trade_no`,\n" +
                "    `trade_body`, `create_time`, `operate_time`, `expire_time`, `process_status`,\n" +
                "    `tracking_no`, `parent_order_id`, `img_url`, `province_id`, `activity_reduce_amount`,\n" +
                "    `coupon_reduce_amount`, `original_total_amount`, `feight_fee`, `feight_fee_reduce`,\n" +
                "    `refundable_time`\n" +
                "from default_catalog.default_database.order_info_full_mq\n" +
                "where create_time is not null;"; // 保留过滤条件
        TableResult tableResult = tableEnv.executeSql(insertSql);
        System.out.println("order_info导入任务启动成功");
        tableResult.await();
    }
}