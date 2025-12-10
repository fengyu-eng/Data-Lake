package com.fy.warehouse.ODS;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.concurrent.ExecutionException;

// 类名对应优惠券信息表：ods_coupon_info_full
public class ods_coupon_info_full {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Configuration config = FlinkConfigUtil.getFlinkConfig();

        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance()
                .withConfiguration(config)
                .inStreamingMode()
                .build());

        //创建mysql-cdc映射表（复用统一集群地址）
        String cdcSql = "CREATE TABLE coupon_info_full_mq (\n" +
                "    `id` bigint NOT NULL COMMENT '购物券编号',\n" +
                "    `coupon_name` STRING NULL COMMENT '购物券名称',\n" +
                "    `coupon_type` STRING NULL COMMENT '购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券',\n" +
                "    `condition_amount` decimal(10,2) NULL COMMENT '满额数（3）',\n" +
                "    `condition_num` bigint NULL COMMENT '满件数（4）',\n" +
                "    `activity_id` bigint NULL COMMENT '活动编号',\n" +
                "    `benefit_amount` decimal(16,2) NULL COMMENT '减金额（1 3）',\n" +
                "    `benefit_discount` decimal(10,2) NULL COMMENT '折扣（2 4）',\n" +
                "    `create_time` timestamp(3) NOT NULL COMMENT '创建时间',\n" +
                "    `range_type` STRING NULL COMMENT '范围类型 1、商品(spuid) 2、品类(三级分类id) 3、品牌',\n" +
                "    `limit_num` int NOT NULL COMMENT '最多领用次数',\n" +
                "    `taken_count` int NOT NULL COMMENT '已领用次数',\n" +
                "    `start_time` timestamp(3) NULL COMMENT '可以领取的开始日期',\n" +
                "    `end_time` timestamp(3) NULL COMMENT '可以领取的结束日期',\n" +
                "    `operate_time` timestamp(3) NOT NULL COMMENT '修改时间',\n" +
                "    `expire_time` timestamp(3) NULL COMMENT '过期时间',\n" +
                "    `range_desc` STRING NULL COMMENT '范围描述',\n" +
                "    PRIMARY KEY(`id`) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'mysql-cdc',\n" +
                "    'scan.startup.mode' = 'initial',\n" + // 复用统一启动模式
                "    'hostname' = '192.168.10.102',\n" + // 统一集群MySQL地址
                "    'port' = '3306',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '123456',\n" + // 统一MySQL密码
                "    'database-name' = 'gmall',\n" +
                "    'table-name' = 'coupon_info',\n" + // 优惠券表名
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

        //创建paimon表（完整保留优惠券表分区配置和所有字段）
        String paimonDDl = "CREATE TABLE IF NOT EXISTS ods.ods_coupon_info_full(\n" +
                "    `id` bigint NOT NULL COMMENT '购物券编号',\n" +
                "    `k1` STRING COMMENT '分区字段',\n" +
                "    `coupon_name` STRING NULL COMMENT '购物券名称',\n" +
                "    `coupon_type` STRING NULL COMMENT '购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券',\n" +
                "    `condition_amount` decimal(10,2) NULL COMMENT '满额数（3）',\n" +
                "    `condition_num` bigint NULL COMMENT '满件数（4）',\n" +
                "    `activity_id` bigint NULL COMMENT '活动编号',\n" +
                "    `benefit_amount` decimal(16,2) NULL COMMENT '减金额（1 3）',\n" +
                "    `benefit_discount` decimal(10,2) NULL COMMENT '折扣（2 4）',\n" +
                "    `create_time` timestamp(3) NOT NULL COMMENT '创建时间',\n" +
                "    `range_type` STRING NULL COMMENT '范围类型 1、商品(spuid) 2、品类(三级分类id) 3、品牌',\n" +
                "    `limit_num` int NOT NULL COMMENT '最多领用次数',\n" +
                "    `taken_count` int NOT NULL COMMENT '已领用次数',\n" +
                "    `start_time` timestamp(3) NULL COMMENT '可以领取的开始日期',\n" +
                "    `end_time` timestamp(3) NULL COMMENT '可以领取的结束日期',\n" +
                "    `operate_time` timestamp(3) NOT NULL COMMENT '修改时间',\n" +
                "    `expire_time` timestamp(3) NULL COMMENT '过期时间',\n" +
                "    `range_desc` STRING NULL COMMENT '范围描述',\n" +
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
        String insertSql = "INSERT INTO ods.ods_coupon_info_full(\n" +
                "    `id`,\n" +
                "    `k1`,\n" +
                "    `coupon_name`,\n" +
                "    `coupon_type`,\n" +
                "    `condition_amount`,\n" +
                "    `condition_num`,\n" +
                "    `activity_id`,\n" +
                "    `benefit_amount`,\n" +
                "    `benefit_discount`,\n" +
                "    `create_time`,\n" +
                "    `range_type`,\n" +
                "    `limit_num`,\n" +
                "    `taken_count`,\n" +
                "    `start_time`,\n" +
                "    `end_time`,\n" +
                "    `operate_time`,\n" +
                "    `expire_time`,\n" +
                "    `range_desc`\n" +
                ")\n" +
                "select\n" +
                "    id,\n" +
                "    DATE_FORMAT(create_time, 'yyyy-MM-dd') AS k1,\n" + // 按创建时间天分区
                "    `coupon_name`,\n" +
                "    `coupon_type`,\n" +
                "    `condition_amount`,\n" +
                "    `condition_num`,\n" +
                "    `activity_id`,\n" +
                "    `benefit_amount`,\n" +
                "    `benefit_discount`,\n" +
                "    `create_time`,\n" +
                "    `range_type`,\n" +
                "    `limit_num`,\n" +
                "    `taken_count`,\n" +
                "    `start_time`,\n" +
                "    `end_time`,\n" +
                "    `operate_time`,\n" +
                "    `expire_time`,\n" +
                "    `range_desc`\n" +
                "from default_catalog.default_database.coupon_info_full_mq\n" +
                "where create_time is not null;"; // 保留过滤条件

        TableResult tableResult = tableEnv.executeSql(insertSql);
        System.out.println("coupon_info导入任务启动成功");
        tableResult.await();
    }
}