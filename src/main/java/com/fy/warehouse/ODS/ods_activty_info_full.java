package com.fy.warehouse.ODS;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.concurrent.ExecutionException;

public class ods_activty_info_full {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //获取Flink集群配置
        Configuration config = FlinkConfigUtil.getFlinkConfig();

        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance()
                .inStreamingMode()
                .withConfiguration(config)
                .build());

        //创建mysql cdc映射表
        String cdcSource = "CREATE TABLE activity_info_full_mq (\n" +
                "    `id` bigint NOT NULL COMMENT '活动id',\n" +
                "    `activity_name` STRING NULL COMMENT '活动名称',\n" +
                "    `activity_type` STRING NULL COMMENT '活动类型',\n" +
                "    `activity_desc` STRING NULL COMMENT '活动描述',\n" +
                "    `start_time` TIMESTAMP(3) NULL COMMENT '开始时间',\n" +
                "    `end_time` TIMESTAMP(3) NULL COMMENT '结束时间',\n" +
                "    `create_time` TIMESTAMP(3) NULL  COMMENT '创建时间',\n" +
                "    PRIMARY KEY(`id`) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'mysql-cdc',\n" +
                "    'scan.startup.mode' = 'initial',\n" +
                "    'hostname' = '192.168.10.102',\n" +
                "    'port' = '3306',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '123456',\n" +
                "    'database-name' = 'gmall',\n" +
                "    'table-name' = 'activity_info',\n" +
                "    'server-time-zone' = 'Asia/Shanghai'\n" +
                ");";
        tableEnv.executeSql(cdcSource);
        System.out.println("MySqlCDC表创建成功");

        //创建paimon-hive Catalog
        String paimon_hive = "CREATE CATALOG  paimon_hive WITH (\n" +
                "    'type' = 'paimon',\n" +
                "    'metastore' = 'hive',\n" +
                "    'uri' = 'thrift://192.168.10.102:9083',\n" +
                "    'warehouse' = 'hdfs://192.168.10.102:8020/user/hive/warehouse'\n" +
                ");";
        tableEnv.executeSql(paimon_hive);
        tableEnv.useCatalog("paimon_hive");
        tableEnv.useDatabase("ods");
        System.out.println("切换到paimon_hive Catalog，并切换到ods库");

        //创建paimon表
        String paimonDDl = "CREATE TABLE IF NOT EXISTS ods.ods_activity_info_full(\n" +
                "    `id`            BIGINT COMMENT '活动id',\n" +
                "    `k1`            STRING COMMENT '分区字段',\n" +
                "    `activity_name` STRING COMMENT '活动名称',\n" +
                "    `activity_type` STRING COMMENT '活动类型',\n" +
                "    `activity_desc` STRING COMMENT '活动描述',\n" +
                "    `start_time`    STRING COMMENT '开始时间',\n" +
                "    `end_time`      STRING COMMENT '结束时间',\n" +
                "    `create_time`   STRING COMMENT '创建时间',\n" +
                "    PRIMARY KEY (`id`,`k1` ) NOT ENFORCED\n" +
                ")   PARTITIONED BY (`k1` ) WITH (\n" +
                "    'connector' = 'paimon',\n" +
                "    'metastore.partitioned-table' = 'true',\n" +
                "    'file.format' = 'parquet',\n" +
                "    'write-buffer-size' = '512mb',\n" +
                "    'write-buffer-spillable' = 'true' ,\n" +
                "    'partition.expiration-time' = '1 d',\n" +
                "    'partition.expiration-check-interval' = '1 h',\n" +
                "    'partition.timestamp-formatter' = 'yyyy-MM-dd',\n" +
                "    'partition.timestamp-pattern' = '$k1'\n" +
                ");";
        tableEnv.executeSql(paimonDDl);
        System.out.println("paimon表创建成功");


        //写入
        String insertSql = "INSERT INTO ods.ods_activity_info_full(`id`, `k1` , `activity_name`, `activity_type`, `activity_desc`, `start_time`, `end_time`, `create_time`)\n" +
                "select\n" +
                "    id,\n" +
                "    DATE_FORMAT(create_time, 'yyyy-MM-dd') AS k1,\n" +
                "    activity_name,\n" +
                "    activity_type,\n" +
                "    activity_desc,\n" +
                "    DATE_FORMAT(start_time, 'yyyy-MM-dd HH:mm:ss') AS start_time,\n" +
                "    DATE_FORMAT(end_time, 'yyyy-MM-dd HH:mm:ss') AS end_time,\n" +
                "    DATE_FORMAT(create_time, 'yyyy-MM-dd HH:mm:ss') AS create_time\n" +
                "from default_catalog.default_database.activity_info_full_mq\n" +
                "where create_time is not null;";
        TableResult tableResult = tableEnv.executeSql(insertSql);
        System.out.println("activity_info导入任务启动成功");
        tableResult.await();

    }
}
