package com.fy.warehouse.ODS;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.concurrent.ExecutionException;

// 类名对应三级品类表：ods_base_category3_full
public class ods_base_category3_full {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Configuration config = FlinkConfigUtil.getFlinkConfig();

        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance()
                .withConfiguration(config)
                .inStreamingMode()
                .build());

        //创建mysql-cdc映射表（复用参考代码的集群地址）
        String cdcSql = "CREATE TABLE base_category3_full_mq (\n" +
                "    `id` bigint NOT NULL COMMENT '编号',\n" +
                "    `name` STRING NOT NULL COMMENT '三级分类名称',\n" +
                "    `category2_id` bigint NULL COMMENT '二级分类编号',\n" +
                "    PRIMARY KEY(`id`) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'mysql-cdc',\n" +
                "    'scan.startup.mode' = 'initial',\n" +
                "    'hostname' = '192.168.10.102',\n" +
                "    'port' = '3306',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '123456',\n" +
                "    'database-name' = 'gmall',\n" +
                "    'table-name' = 'base_category3',\n" + // 三级品类表名
                "    'server-time-zone' = 'Asia/Shanghai'\n" +
                ");";
        tableEnv.executeSql(cdcSql);
        System.out.println("MySql CDC表创建成功");

        //创建catalog（完全复用参考代码的集群地址）
        String catalogSql = "CREATE CATALOG paimon_hive WITH (\n" +
                "    'type' = 'paimon',\n" +
                "    'metastore' = 'hive',\n" +
                "    'uri' = 'thrift://192.168.10.102:9083',\n" + // 复用参考代码的Hive URI
                "    'warehouse' = 'hdfs://192.168.10.102/user/hive/warehouse'\n" + // 复用参考代码的HDFS路径
                ");";

        tableEnv.executeSql(catalogSql);
        System.out.println("创建catalog成功");

        tableEnv.useCatalog("paimon_hive");
        tableEnv.useDatabase("ods"); // 复用参考代码的数据库切换逻辑
        System.out.println("切换到ods库，paimon_hive Catalog");

        //创建paimon表（三级品类表结构，修正原SQL注释错误）
        String paimonDDl = "CREATE TABLE IF NOT EXISTS ods.ods_base_category3_full(\n" +
                "    `id` bigint NOT NULL COMMENT '编号',\n" +
                "    `name` STRING NOT NULL COMMENT '三级分类名称',\n" + // 修正原SQL注释（原为"二级分类名称"）
                "    `category2_id` bigint NULL COMMENT '二级分类编号',\n" +
                "    PRIMARY KEY (`id`) NOT ENFORCED\n" +
                ");";
        tableEnv.executeSql(paimonDDl);
        System.out.println("paimon表创建成功");

        //导入任务（三级品类表字段同步）
        String insertSql = "INSERT INTO ods.ods_base_category3_full(`id`, `name`, `category2_id`)\n" +
                "select\n" +
                "    id,\n" +
                "    name,\n" +
                "    category2_id\n" +
                "from default_catalog.default_database.base_category3_full_mq;";

        TableResult tableResult = tableEnv.executeSql(insertSql);
        System.out.println("base_category3导入任务启动成功");
        tableResult.await();
    }
}