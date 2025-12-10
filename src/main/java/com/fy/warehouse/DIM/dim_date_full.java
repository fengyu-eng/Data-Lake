package com.fy.warehouse.DIM;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.concurrent.ExecutionException;

public class dim_date_full {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Configuration config = FlinkConfigUtil.getFlinkConfig();

        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance()
                .withConfiguration(config)
                .build());

        //创建cdc表
        String cdcSql = "CREATE TABLE dim_date_full (\n" +
                "    `date_id`    VARCHAR(255) COMMENT '日期ID',\n" +
                "    `week_id`    int COMMENT '周ID,一年中的第几周',\n" +
                "    `week_day`   int COMMENT '周几',\n" +
                "    `day`        int COMMENT '每月的第几天',\n" +
                "    `month`      int COMMENT '一年中的第几月',\n" +
                "    `quarter`    int COMMENT '一年中的第几季度',\n" +
                "    `year`       int COMMENT '年份',\n" +
                "    `is_workday` int COMMENT '是否是工作日',\n" +
                "    `holiday_id` VARCHAR(255) COMMENT '节假日',\n" +
                "    PRIMARY KEY(`date_id`) NOT ENFORCED\n" +
                ") WITH (\n" +
                "      'connector' = 'mysql-cdc',\n" +
                "      'scan.startup.mode' = 'initial',\n" +
                "      'hostname' = '192.168.10.102',\n" +
                "      'port' = '3306',\n" +
                "      'username' = 'root',\n" +
                "      'password' = '123456',\n" +
                "      'database-name' = 'gmall',\n" +
                "      'table-name' = 'dim_date',\n" +
                "      'server-time-zone' = 'Asia/Shanghai'\n" +
                "      );";

        tableEnv.executeSql(cdcSql);
        System.out.println("cdc表创建成功");

        //创建catalog
        String catalogSql = "CREATE CATALOG paimon_hive WITH (\n" +
                "    'type' = 'paimon',\n" +
                "    'metastore' = 'hive',\n" +
                "    'uri' = 'thrift://192.168.10.102:9083',\n" +
                "    'warehouse' = 'hdfs://192.168.10.102/user/hive/warehouse'\n" +
                ");";

        tableEnv.executeSql(catalogSql);
        System.out.println("catalog创建成功");

        tableEnv.useCatalog("paimon_hive");
        tableEnv.useDatabase("dim");

        //创建paimon表
        String paimonSql = "CREATE TABLE IF NOT EXISTS dim.dim_date_full(\n" +
                "    `date_id`    VARCHAR(255) COMMENT '日期ID',\n" +
                "    `week_id`    int COMMENT '周ID,一年中的第几周',\n" +
                "    `week_day`   int COMMENT '周几',\n" +
                "    `day`        int COMMENT '每月的第几天',\n" +
                "    `month`      int COMMENT '一年中的第几月',\n" +
                "    `quarter`    int COMMENT '一年中的第几季度',\n" +
                "    `year`       int COMMENT '年份',\n" +
                "    `is_workday` int COMMENT '是否是工作日',\n" +
                "    `holiday_id` VARCHAR(255) COMMENT '节假日',\n" +
                "    PRIMARY KEY (`date_id` ) NOT ENFORCED\n" +
                "    );";

        tableEnv.executeSql(paimonSql);
        System.out.println("paimon表创建成功");

        //导入
        String insertSQL = "INSERT INTO dim.dim_date_full(\n" +
                "    `date_id`,\n" +
                "    `week_id`,\n" +
                "    `week_day`,\n" +
                "    `day`,\n" +
                "    `month`,\n" +
                "    `quarter`,\n" +
                "    `year`,\n" +
                "    `is_workday`,\n" +
                "    `holiday_id`\n" +
                "    )\n" +
                "select\n" +
                "    `date_id`,\n" +
                "    `week_id`,\n" +
                "    `week_day`,\n" +
                "    `day`,\n" +
                "    `month`,\n" +
                "    `quarter`,\n" +
                "    `year`,\n" +
                "    `is_workday`,\n" +
                "    `holiday_id`\n" +
                "from default_catalog.default_database.dim_date_full;";

        TableResult tableResult = tableEnv.executeSql(insertSQL);
        System.out.println("dim_date_full任务启动成功");
        tableResult.await();
    }
}
