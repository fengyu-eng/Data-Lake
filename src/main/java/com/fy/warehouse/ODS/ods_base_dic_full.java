package com.fy.warehouse.ODS;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.concurrent.ExecutionException;

// 类名对应字典表：ods_base_dic_full
public class ods_base_dic_full {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Configuration config = FlinkConfigUtil.getFlinkConfig();

        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance()
                .withConfiguration(config)
                .inStreamingMode()
                .build());

        //创建mysql-cdc映射表（复用统一集群地址）
        String cdcSql = "CREATE TABLE base_dic_full_mq (\n" +
                "    `dic_code` STRING NOT NULL COMMENT '编号',\n" +
                "    `dic_name` STRING NULL COMMENT '编码名称',\n" +
                "    `parent_code` STRING NULL COMMENT '父编号',\n" +
                "    `create_time` TIMESTAMP(3) NULL COMMENT '创建时间',\n" +
                "    `operate_time` TIMESTAMP(3) NULL COMMENT '修改日期',\n" +
                "    PRIMARY KEY(`dic_code`) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'mysql-cdc',\n" +
                "    'scan.startup.mode' = 'initial',\n" + // 复用统一启动模式
                "    'hostname' = '192.168.10.102',\n" + // 统一集群MySQL地址
                "    'port' = '3306',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '123456',\n" + // 统一MySQL密码
                "    'database-name' = 'gmall',\n" +
                "    'table-name' = 'base_dic',\n" + // 字典表名
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

        //创建paimon表（完整保留字典表分区配置和字段）
        String paimonDDl = "CREATE TABLE IF NOT EXISTS ods.ods_base_dic_full(\n" +
                "    `dic_code` STRING NOT NULL COMMENT '编号',\n" +
                "    `k1` STRING COMMENT '分区字段',\n" +
                "    `dic_name` STRING NULL COMMENT '编码名称',\n" +
                "    `parent_code` STRING NULL COMMENT '父编号',\n" +
                "    `create_time` STRING NULL COMMENT '创建时间',\n" +
                "    `operate_time` STRING NULL COMMENT '修改日期',\n" +
                "    PRIMARY KEY (`dic_code`,`k1`) NOT ENFORCED\n" + // 复合主键（含分区字段）
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

        //导入任务（完整保留字段转换和分区逻辑，修正原SQL字段名错误）
        String insertSql = "INSERT INTO ods.ods_base_dic_full(\n" +
                "    `dic_code`,\n" +
                "    `k1`,\n" +
                "    `dic_name`,\n" +
                "    `parent_code`,\n" +
                "    `create_time`,\n" +
                "    `operate_time`\n" +
                ")\n" +
                "select\n" +
                "    `dic_code`,\n" +
                "    DATE_FORMAT(create_time, 'yyyy-MM-dd') AS k1,\n" + // 分区字段：创建时间按天分区
                "    `dic_name`,\n" +
                "    `parent_code`,\n" +
                "    DATE_FORMAT(create_time, 'yyyy-MM-dd HH:mm:ss') AS create_time,\n" + // 修正原SQL别名错误（原为start_time）
                "    DATE_FORMAT(operate_time, 'yyyy-MM-dd HH:mm:ss') AS operate_time\n" + // 修正原SQL别名错误（原为start_time）
                "from default_catalog.default_database.base_dic_full_mq;";

        TableResult tableResult = tableEnv.executeSql(insertSql);
        System.out.println("base_dic导入任务启动成功");
        tableResult.await();
    }
}