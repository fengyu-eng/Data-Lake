package com.fy.warehouse.ODS;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.concurrent.ExecutionException;

// 类名修改为对应一级品类表：ods_base_category1_full
public class ods_base_category1_full {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Configuration config = FlinkConfigUtil.getFlinkConfig();

        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance()
                .withConfiguration(config)
                .inStreamingMode()
                .build());

        //创建mysql-cdc映射表
        String cdcSql = "CREATE TABLE base_category1_full_mq (\n" +
                "    `id` bigint NOT NULL COMMENT '编号',\n" +
                "    `name` STRING NOT NULL COMMENT '分类名称',\n" +
                "    PRIMARY KEY(`id`) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'mysql-cdc',\n" +
                "    'scan.startup.mode' = 'initial',\n" +
                "    'hostname' = '192.168.10.102',\n" +
                "    'port' = '3306',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '123456',\n" +
                "    'database-name' = 'gmall',\n" +
                "    'table-name' = 'base_category1',\n" +
                "    'server-time-zone' = 'Asia/Shanghai'\n" +
                ");";
        tableEnv.executeSql(cdcSql);
        System.out.println("MySql CDC表创建成功");

        //创建catalog
        String catalogSql = "CREATE CATALOG paimon_hive WITH (\n" +
                "    'type' = 'paimon',\n" +
                "    'metastore' = 'hive',\n" +
                "    'uri' = 'thrift://192.168.10.102:9083',\n" +
                "    'warehouse' = 'hdfs://192.168.10.102/user/hive/warehouse'\n" +
                ");";

        tableEnv.executeSql(catalogSql);
        System.out.println("创建catalog成功");

        tableEnv.useCatalog("paimon_hive");
        // 执行创建ods数据库（对应你提供的SQL）
        tableEnv.useDatabase("ods");
        System.out.println("切换到ods库，paimon_hive Catalog");

        //创建paimon表
        String paimonDDl = "CREATE TABLE IF NOT EXISTS ods.ods_base_category1_full(\n" +
                "    `id`   bigint COMMENT '编号',\n" +
                "    `name` STRING COMMENT '分类名称',\n" +
                "    PRIMARY KEY (`id`) NOT ENFORCED\n" +
                ");";
        tableEnv.executeSql(paimonDDl);
        System.out.println("paimon表创建成功");

        //导入任务
        String insertSql = "INSERT INTO ods.ods_base_category1_full(`id`, `name`)\n" +
                "select\n" +
                "    id,\n" +
                "    name\n" +
                "from default_catalog.default_database.base_category1_full_mq;";

        TableResult tableResult = tableEnv.executeSql(insertSql);
        // 修正日志输出，对应一级品类表
        System.out.println("base_category1导入任务启动成功");
        tableResult.await();
    }
}