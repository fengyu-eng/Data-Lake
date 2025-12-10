package com.fy.warehouse.ODS;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.concurrent.ExecutionException;

// 类名对应省份表：ods_base_province_full
public class ods_base_province_full {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Configuration config = FlinkConfigUtil.getFlinkConfig();

        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance()
                .withConfiguration(config)
                .inStreamingMode()
                .build());

        //创建mysql-cdc映射表（复用统一集群地址）
        String cdcSql = "CREATE TABLE base_province_full_mq (\n" +
                "    `id` bigint NOT NULL COMMENT 'id',\n" +
                "    `name` STRING NULL COMMENT '省名称',\n" +
                "    `region_id` STRING NULL COMMENT '大区id',\n" +
                "    `area_code` STRING NULL COMMENT '行政区位码',\n" +
                "    `iso_code` STRING NULL COMMENT '国际编码',\n" +
                "    `iso_3166_2` STRING NULL COMMENT 'ISO3166编码',\n" +
                "    PRIMARY KEY(`id`) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'mysql-cdc',\n" +
                "    'scan.startup.mode' = 'initial',\n" + // 复用统一启动模式
                "    'hostname' = '192.168.10.102',\n" + // 统一集群MySQL地址
                "    'port' = '3306',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '123456',\n" + // 统一MySQL密码
                "    'database-name' = 'gmall',\n" +
                "    'table-name' = 'base_province',\n" + // 省份表名
                "    'server-time-zone' = 'Asia/Shanghai',\n" +
                "    'scan.incremental.snapshot.chunk.key-column' = 'id'\n" +
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

        //创建paimon表（完整保留省份表字段结构）
        String paimonDDl = "CREATE TABLE IF NOT EXISTS ods.ods_base_province_full(\n" +
                "    `id` bigint NOT NULL COMMENT 'id',\n" +
                "    `name` STRING NULL COMMENT '省名称',\n" +
                "    `region_id` STRING NULL COMMENT '大区id',\n" +
                "    `area_code` STRING NULL COMMENT '行政区位码',\n" +
                "    `iso_code` STRING NULL COMMENT '国际编码',\n" +
                "    `iso_3166_2` STRING NULL COMMENT 'ISO3166编码',\n" +
                "    PRIMARY KEY (`id`) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'paimon'\n" +
                ");";

        tableEnv.executeSql(paimonDDl);
        System.out.println("paimon表创建成功");

        //导入任务（完整保留省份表所有字段同步逻辑）
        String insertSql = "INSERT INTO ods.ods_base_province_full(\n" +
                "    `id`,\n" +
                "    `name`,\n" +
                "    `region_id`,\n" +
                "    `area_code`,\n" +
                "    `iso_code`,\n" +
                "    `iso_3166_2`\n" +
                ")\n" +
                "select\n" +
                "    `id`,\n" +
                "    `name`,\n" +
                "    `region_id`,\n" +
                "    `area_code`,\n" +
                "    `iso_code`,\n" +
                "    `iso_3166_2`\n" +
                "from default_catalog.default_database.base_province_full_mq;";

        TableResult tableResult = tableEnv.executeSql(insertSql);
        System.out.println("base_province导入任务启动成功");
        tableResult.await();
    }
}