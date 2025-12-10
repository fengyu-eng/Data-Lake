package com.fy.warehouse.ODS;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.paimon.hive.HiveCatalog;

import java.util.concurrent.ExecutionException;

public class ods_log_inc {
    public static void main(String[] args) throws ExecutionException, InterruptedException {;
        //获取Flink配置
        Configuration config = FlinkConfigUtil.getFlinkConfig();

        // 2. 基于完整配置创建 TableEnvironment（这是核心修复点）
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance()
                .withConfiguration(config)
                .inStreamingMode()
                .build());

        String kafkaSourceDDL = "CREATE TABLE kafka_source (\n" +
                "    kafka_partition INT METADATA FROM 'partition',\n" +
                "    kafka_offset INT METADATA FROM 'offset',\n" +
                "    kafka_timestamp TIMESTAMP(3) METADATA FROM 'timestamp',\n" +
                "    `common` ROW<ar STRING, ba STRING, ch STRING, is_new STRING, md STRING, mid STRING, os STRING, uid STRING, vc STRING> NULL,\n" +
                "    `start`  ROW<entry STRING, loading_time STRING, open_ad_id STRING , open_ad_ms STRING, open_ad_skip_ms STRING> NULL,\n" +
                "    `page`   ROW<during_time BIGINT, item STRING, item_type STRING, last_page_id  STRING ,page_id STRING, source_type STRING> NULL,\n" +
                "    `actions` STRING,\n" +
                "    `displays` STRING,\n" +
                "    `err` ROW<error_code BIGINT, msg STRING>,\n" +
                "    `ts` BIGINT\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'ODS_BASE_LOG',\n" +
                "    'properties.bootstrap.servers' = '192.168.10.102:9092,192.168.10.103:9092,192.168.10.104:9092',\n" +
                "    'properties.group.id' = 'ODS_BASE_LOG',\n" +
                "    -- 'scan.startup.mode' = 'group-offsets',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'properties.enable.auto.commit'='true',\n" +
                "    'properties.auto.commit.interval.ms'='5000',\n" +
                "    'format' = 'json',\n" +
                "    'json.ignore-parse-errors' = 'true',\n" +
                "    'json.fail-on-missing-field' = 'false'\n" +
                ");";
        tableEnv.executeSql(kafkaSourceDDL);
        System.out.println("Kafka源表注册成功");
        // 2. 注册 Print 输出表（内置连接器，打印到控制台）
//        String printSinkDDL = "CREATE TABLE print_sink (\n" +
//                "    kafka_partition INT,\n" +
//                "    kafka_offset INT,\n" +
//                "    kafka_timestamp TIMESTAMP(3),\n" +
//                "    `common` ROW<ar STRING, ba STRING, ch STRING, is_new STRING, md STRING, mid STRING, os STRING, uid STRING, vc STRING> NULL,\n" +
//                "    `start`  ROW<entry STRING, loading_time STRING, open_ad_id STRING , open_ad_ms STRING, open_ad_skip_ms STRING> NULL,\n" +
//                "    `page`   ROW<during_time BIGINT, item STRING, item_type STRING, last_page_id  STRING ,page_id STRING, source_type STRING> NULL,\n" +
//                "    `actions` STRING,\n" +
//                "    `displays` STRING,\n" +
//                "    `err` ROW<error_code BIGINT, msg STRING>,\n" +
//                "    `ts` BIGINT\n" +
//                ") WITH (\n" +
//                "    'connector' = 'print',\n" +
//                "    'print-identifier' = 'KAFKA_LOG_DATA', -- 控制台输出前缀（区分其他打印）\n" +
//                "    'sink.parallelism' = '1' -- 并行度设为 1，避免输出乱序（本地测试用）\n" +
//                ");";
//        tableEnv.executeSql(printSinkDDL);
//
//        // 3. 将 Kafka 源表数据插入到 Print 表（触发流处理，控制台打印数据）
//        tableEnv.executeSql("INSERT INTO print_sink SELECT * FROM kafka_source");

        tableEnv.executeSql(
                "CREATE CATALOG paimon_hive WITH (\n" +
                        "    'type' = 'paimon',\n" +
                        "    'metastore' = 'hive',\n" +
                        "    'uri' = 'thrift://192.168.10.102:9083',\n" +
                        "    'warehouse' = 'hdfs://192.168.10.102:8020/user/hive/warehouse'\n" +
                        ")"
        );
        System.out.println("创建paimon_hive catalog成功");
        tableEnv.useCatalog("paimon_hive");
        tableEnv.executeSql("create database if not exists ods");
        tableEnv.useDatabase("ods");
        System.out.println("Paimon数据库ods创建并切换成功,catalog切换成功");


        String paimonSinkDDL = "CREATE TABLE IF NOT EXISTS ods_log_inc (" +
                "`id` STRING," +
                "`k1` STRING," +
                "`common_ar` STRING," +
                "`common_ba` STRING," +
                "`common_ch` STRING," +
                "`common_is_new` STRING," +
                "`common_md` STRING," +
                "`common_mid` STRING," +
                "`common_os` STRING," +
                "`common_uid` STRING," +
                "`common_vc` STRING," +
                "`start_entry` STRING," +
                "`start_loading_time` STRING," +
                "`start_open_ad_id` STRING," +
                "`start_open_ad_ms` STRING," +
                "`start_open_ad_skip_ms` STRING," +
                "`page_during_time` BIGINT," +
                "`page_item` STRING," +
                "`page_item_type` STRING," +
                "`page_last_page_id` STRING," +
                "`page_page_id` STRING," +
                "`page_source_type` STRING," +
                "`actions` STRING COMMENT '动作信息'," +
                "`displays` STRING COMMENT '曝光信息'," +
                "`err_error_code` BIGINT," +
                "`err_msg` STRING," +
                "`ts` BIGINT COMMENT '时间戳'," +
                "PRIMARY KEY (`id`) NOT ENFORCED" +
                ") WITH (" +
                "'connector' = 'paimon'," +
                "'metastore.partitioned-table' = 'false'," + // 原 SQL 未显式分区，默认关闭（若需分区可改为 true）
                "'file.format' = 'parquet'," + // 文件格式默认 parquet，可按需修改
                "'write-buffer-size' = '512mb'," + // 优化写入性能：写缓冲区 512MB
                "'write-buffer-spillable' = 'true'" + // 缓冲区满时溢出到磁盘
                ")";

        tableEnv.executeSql(paimonSinkDDL);
        System.out.println("paimon目标表ods_log_inc创建成功");



        String insertSql = "INSERT INTO ods_log_inc (" +
                "`id`, `k1`, `common_ar`, `common_ba`, `common_ch`, `common_is_new`, `common_md`, `common_mid`, `common_os`, `common_uid`, `common_vc`, " +
                "`start_entry`, `start_loading_time`, `start_open_ad_id`, `start_open_ad_ms`, `start_open_ad_skip_ms`, " +
                "`page_during_time`, `page_item`, `page_item_type`, `page_last_page_id`, `page_page_id`, `page_source_type`, " +
                "`actions`, `displays`, `err_error_code`, `err_msg`, `ts`" +
                ") SELECT " +
                "CONCAT(cast(kafka_partition as string), cast(kafka_offset as string), cast(kafka_timestamp as string)) as id," +
                "DATE_FORMAT(FROM_UNIXTIME(cast(ts / 1000 as BIGINT)), 'yyyy-MM-dd') AS k1," +
                "`common`.ar," +
                "`common`.ba," +
                "`common`.ch," +
                "`common`.is_new," +
                "`common`.md," +
                "`common`.mid," +
                "`common`.os," +
                "`common`.uid," +
                "`common`.vc," +
                "`start`.entry," +
                "`start`.loading_time," +
                "`start`.open_ad_id," +
                "`start`.open_ad_ms," +
                "`start`.open_ad_skip_ms," +
                "`page`.during_time," +
                "`page`.item," +
                "`page`.item_type," +
                "`page`.last_page_id," +
                "`page`.page_id," +
                "`page`.source_type," +
                "`actions`," +
                "`displays`," +
                "`err`.error_code," +
                "`err`.msg," +
                "`ts`" +
                " FROM default_catalog.default_database.kafka_source";

        TableResult tableResult = tableEnv.executeSql(insertSql);
        System.out.println("Kafka 日志写入 Paimon 任务启动成功！");
        tableResult.await();
    }
}
