package com.fy.warehouse.ODS;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.concurrent.ExecutionException;

// 类名对应SPU表：ods_spu_info_full
public class ods_spu_info_full {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Configuration config = FlinkConfigUtil.getFlinkConfig();

        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance()
                .withConfiguration(config)
                .inStreamingMode()
                .build());

        // 创建mysql-cdc映射表（复用统一集群地址）
        String cdcSql = "CREATE TABLE spu_info_full_mq (\n" +
                "    `id` bigint NOT NULL COMMENT '商品id',\n" +
                "    `spu_name` string NULL COMMENT '商品名称',\n" +
                "    `description` string NULL COMMENT '商品描述(后台简述）',\n" +
                "    `category3_id` bigint NULL COMMENT '三级分类id',\n" +
                "    `tm_id` bigint NULL COMMENT '品牌id',\n" +
                "    PRIMARY KEY(`id`) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'mysql-cdc',\n" +
                "    'scan.startup.mode' = 'initial',\n" + // 统一启动模式（全量表适用）
                "    'hostname' = '192.168.10.102',\n" + // 统一MySQL地址
                "    'port' = '3306',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '123456',\n" + // 统一MySQL密码
                "    'database-name' = 'gmall',\n" +
                "    'table-name' = 'spu_info',\n" + // 源表名
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

        // 创建paimon表（完整保留全量表配置，无分区）
        String paimonDDl = "CREATE TABLE IF NOT EXISTS ods.ods_spu_info_full(\n" +
                "    `id` bigint NOT NULL COMMENT '商品id',\n" +
                "    `spu_name` string NULL COMMENT '商品名称',\n" +
                "    `description` string NULL COMMENT '商品描述(后台简述）',\n" +
                "    `category3_id` bigint NULL COMMENT '三级分类id',\n" +
                "    `tm_id` bigint NULL COMMENT '品牌id',\n" +
                "    PRIMARY KEY (`id`) NOT ENFORCED\n" + // 单字段主键（全量表无分区）
                ") WITH (\n" +
                "    'connector' = 'paimon',\n" +
                "    'metastore.partitioned-table' = 'false',\n" + // 全量表关闭分区
                "    'file.format' = 'parquet',\n" +
                "    'write-buffer-size' = '512mb',\n" +
                "    'write-buffer-spillable' = 'true'\n" + // 保留缓存配置
                ");";
        tableEnv.executeSql(paimonDDl);
        System.out.println("paimon表创建成功");

        // 导入任务（全量表无分区，直接同步所有字段）
        String insertSql = "INSERT INTO ods.ods_spu_info_full(\n" +
                "    `id`, `spu_name`, `description`, `category3_id`, `tm_id`\n" +
                ")\n" +
                "select\n" +
                "    `id`, `spu_name`, `description`, `category3_id`, `tm_id`\n" +
                "from default_catalog.default_database.spu_info_full_mq;"; // 全量表无过滤条件
        TableResult tableResult = tableEnv.executeSql(insertSql);
        System.out.println("spu_info导入任务启动成功");
        tableResult.await();
    }
}