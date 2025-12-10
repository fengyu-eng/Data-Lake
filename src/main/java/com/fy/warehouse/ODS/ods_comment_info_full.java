package com.fy.warehouse.ODS;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.concurrent.ExecutionException;

// 类名对应评论表：ods_comment_info_full
public class ods_comment_info_full {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Configuration config = FlinkConfigUtil.getFlinkConfig();

        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance()
                .withConfiguration(config)
                .inStreamingMode()
                .build());

        //创建mysql-cdc映射表（复用统一集群地址）
        String cdcSql = "CREATE TABLE comment_info_full_mq (\n" +
                "    `id` bigint NOT NULL COMMENT '编号',\n" +
                "    `user_id` bigint NULL COMMENT '用户id',\n" +
                "    `nick_name` STRING NULL COMMENT '用户昵称',\n" +
                "    `head_img` STRING NULL,\n" +
                "    `sku_id` bigint NULL COMMENT 'skuid',\n" +
                "    `spu_id` bigint NULL COMMENT '商品id',\n" +
                "    `order_id` bigint NULL COMMENT '订单编号',\n" +
                "    `appraise` STRING NULL COMMENT '评价 1 好评 2 中评 3 差评',\n" +
                "    `comment_txt` STRING NULL COMMENT '评价内容',\n" +
                "    `create_time` timestamp(3) NOT NULL COMMENT '创建时间',\n" +
                "    `operate_time` timestamp(3) NOT NULL COMMENT '修改时间',\n" +
                "    PRIMARY KEY(`id`) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'mysql-cdc',\n" +
                "    'scan.startup.mode' = 'initial',\n" + // 复用统一启动模式
                "    'hostname' = '192.168.10.102',\n" + // 统一集群MySQL地址
                "    'port' = '3306',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '123456',\n" + // 统一MySQL密码
                "    'database-name' = 'gmall',\n" +
                "    'table-name' = 'comment_info',\n" + // 评论表名
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

        //创建paimon表（完整保留评论表分区配置和所有字段）
        String paimonDDl = "CREATE TABLE IF NOT EXISTS ods.ods_comment_info_full(\n" +
                "    `id` bigint NOT NULL COMMENT '编号',\n" +
                "    `k1` STRING COMMENT '分区字段',\n" +
                "    `user_id` bigint NULL COMMENT '用户id',\n" +
                "    `nick_name` STRING NULL COMMENT '用户昵称',\n" +
                "    `head_img` STRING NULL,\n" +
                "    `sku_id` bigint NULL COMMENT 'skuid',\n" +
                "    `spu_id` bigint NULL COMMENT '商品id',\n" +
                "    `order_id` bigint NULL COMMENT '订单编号',\n" +
                "    `appraise` STRING NULL COMMENT '评价 1 好评 2 中评 3 差评',\n" +
                "    `comment_txt` STRING NULL COMMENT '评价内容',\n" +
                "    `create_time` timestamp(3) NOT NULL COMMENT '创建时间',\n" +
                "    `operate_time` timestamp(3) NOT NULL COMMENT '修改时间',\n" +
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
        String insertSql = "INSERT INTO ods.ods_comment_info_full(\n" +
                "    `id`,\n" +
                "    `k1`,\n" +
                "    `user_id`,\n" +
                "    `nick_name`,\n" +
                "    `head_img`,\n" +
                "    `sku_id`,\n" +
                "    `spu_id`,\n" +
                "    `order_id`,\n" +
                "    `appraise`,\n" +
                "    `comment_txt`,\n" +
                "    `create_time`,\n" +
                "    `operate_time`\n" +
                ")\n" +
                "select\n" +
                "    id,\n" +
                "    DATE_FORMAT(create_time, 'yyyy-MM-dd') AS k1,\n" + // 按创建时间天分区
                "    `user_id`,\n" +
                "    `nick_name`,\n" +
                "    `head_img`,\n" +
                "    `sku_id`,\n" +
                "    `spu_id`,\n" +
                "    `order_id`,\n" +
                "    `appraise`,\n" +
                "    `comment_txt`,\n" +
                "    `create_time`,\n" +
                "    `operate_time`\n" +
                "from default_catalog.default_database.comment_info_full_mq\n" +
                "where create_time is not null;"; // 保留过滤条件

        TableResult tableResult = tableEnv.executeSql(insertSql);
        System.out.println("comment_info导入任务启动成功");
        tableResult.await();
    }
}