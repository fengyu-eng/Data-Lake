package com.fy.warehouse.ODS;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.concurrent.ExecutionException;

public class ods_activity_rule_full {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Configuration config = FlinkConfigUtil.getFlinkConfig();

        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance()
                .withConfiguration(config)
                .inStreamingMode()
                .build());

        //创建mysql-cdc映射表
        String cdcSql = "CREATE TABLE activity_rule_full_mq (\n" +
                "   `id` int NOT NULL  COMMENT '编号',\n" +
                "   `activity_id` int  NULL COMMENT '类型',\n" +
                "   `activity_type` STRING  NULL COMMENT '活动类型',\n" +
                "   `condition_amount` decimal(16,2)  NULL COMMENT '满减金额',\n" +
                "   `condition_num` BIGINT  NULL COMMENT '满减件数',\n" +
                "   `benefit_amount` decimal(16,2)  NULL COMMENT '优惠金额',\n" +
                "   `benefit_discount` decimal(10,2)  NULL COMMENT '优惠折扣',\n" +
                "   `benefit_level` BIGINT  NULL COMMENT '优惠级别',\n" +
                "    PRIMARY KEY(`id`) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'mysql-cdc',\n" +
                "    'scan.startup.mode' = 'initial',\n" +
                "    'hostname' = '192.168.10.102',\n" +
                "    'port' = '3306',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '123456',\n" +
                "    'database-name' = 'gmall',\n" +
                "    'table-name' = 'activity_rule',\n" +
                "    'server-time-zone' = 'Asia/Shanghai'\n" +
                "      );";
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
        tableEnv.useDatabase("ods");
        System.out.println("切换到ods库，paimon_hive Catalog");

        //创建paimon表
        String paimonDDl = "CREATE TABLE IF NOT EXISTS ods.ods_activity_rule_full(\n" +
                "    `id`               INT COMMENT '编号',\n" +
                "    `activity_id`      INT COMMENT '类型',\n" +
                "    `activity_type`    STRING COMMENT '活动类型',\n" +
                "    `condition_amount` DECIMAL(16, 2) COMMENT '满减金额',\n" +
                "    `condition_num`    BIGINT COMMENT '满减件数',\n" +
                "    `benefit_amount`   DECIMAL(16, 2) COMMENT '优惠金额',\n" +
                "    `benefit_discount` DECIMAL(16, 2) COMMENT '优惠折扣',\n" +
                "    `benefit_level`    BIGINT COMMENT '优惠级别',\n" +
                "    PRIMARY KEY (`id`) NOT ENFORCED\n" +
                "    );";
        tableEnv.executeSql(paimonDDl);
        System.out.println("paimon表创建成功");

        //导入任务
        String insertSql = "INSERT INTO ods.ods_activity_rule_full(\n" +
                "    `id`,\n" +
                "    `activity_id`,\n" +
                "    `activity_type`,\n" +
                "    `condition_amount`,\n" +
                "    `condition_num`,\n" +
                "    `benefit_amount`,\n" +
                "    `benefit_discount`,\n" +
                "    `benefit_level`\n" +
                ")\n" +
                "select\n" +
                "    `id`,\n" +
                "    `activity_id`,\n" +
                "    `activity_type`,\n" +
                "    `condition_amount`,\n" +
                "    `condition_num`,\n" +
                "    `benefit_amount`,\n" +
                "    `benefit_discount`,\n" +
                "    `benefit_level`\n" +
                "from default_catalog.default_database.activity_rule_full_mq;";

        TableResult tableResult = tableEnv.executeSql(insertSql);
        System.out.println("activity_rull导入任务启动成功");
        tableResult.await();
    }
}
