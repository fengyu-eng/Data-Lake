package com.fy.warehouse.dws;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

// 省份订单30日汇总表
public class dws_trade_province_order_nd_full {
    public static void main(String[] args) {
        // 获取Flink配置
        Configuration config = FlinkConfigUtil.getFlinkConfig();
        // 创建TableEnvironment
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance()
                .withConfiguration(config)
                .build());

        // 创建catalog
        String catalogSql = "CREATE CATALOG paimon_hive WITH (\n" +
                "    'type' = 'paimon',\n" +
                "    'metastore' = 'hive',\n" +
                "    'uri' = 'thrift://192.168.10.102:9083',\n" +
                "    'warehouse' = 'hdfs://192.168.10.102/user/hive/warehouse'\n" +
                ");";
        tableEnv.executeSql(catalogSql);
        System.out.println("catalog创建成功");

        // 使用指定的catalog和database
        tableEnv.useCatalog("paimon_hive");
        tableEnv.executeSql("create DATABASE IF NOT EXISTS dws;");
        tableEnv.useDatabase("dws");

        // 创建paimon表
        String paimonSql = "CREATE TABLE IF NOT EXISTS dws.dws_trade_province_order_nd_full(\n" +
                "    `province_id`                BIGINT COMMENT '省份id',\n" +
                "    `k1`                         STRING COMMENT '分区字段',\n" +
                "    `province_name`              STRING COMMENT '省份名称',\n" +
                "    `area_code`                  STRING COMMENT '地区编码',\n" +
                "    `iso_code`                   STRING COMMENT '旧版ISO-3166-2编码',\n" +
                "    `iso_3166_2`                 STRING COMMENT '新版版ISO-3166-2编码',\n" +
                "    `order_count_30d`            BIGINT COMMENT '最近30日下单次数',\n" +
                "    `order_original_amount_30d`  DECIMAL(16, 2) COMMENT '最近30日下单原始金额',\n" +
                "    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '最近30日下单活动优惠金额',\n" +
                "    `coupon_reduce_amount_30d`   DECIMAL(16, 2) COMMENT '最近30日下单优惠券优惠金额',\n" +
                "    `order_total_amount_30d`     DECIMAL(16, 2) COMMENT '最近30日下单最终金额',\n" +
                "    PRIMARY KEY (`province_id`,`k1` ) NOT ENFORCED\n" +
                "    )   PARTITIONED BY (`k1` ) WITH (\n" +
                "    'connector' = 'paimon',\n" +
                "    'metastore.partitioned-table' = 'true',\n" +
                "    'file.format' = 'parquet',\n" +
                "    'write-buffer-size' = '512mb',\n" +
                "    'write-buffer-spillable' = 'true' ,\n" +
                "    'partition.expiration-time' = '1 d',\n" +
                "    'partition.expiration-check-interval' = '1 h',\n" +
                "    'partition.timestamp-formatter' = 'yyyy-MM-dd',\n" +
                "    'partition.timestamp-pattern' = '$k1'\n" +
                "    );";

        tableEnv.executeSql(paimonSql);
        System.out.println("paimon表创建成功");

        // 数据插入（补充30天日期筛选条件）
        String insertSql = "insert into dws.dws_trade_province_order_nd_full select\n" +
                "    `province_id`               ,--BIGINT COMMENT '省份id',\n" +
                "    `k1`                        ,--STRING COMMENT '分区字段',\n" +
                "    `province_name`             ,--STRING COMMENT '省份名称',\n" +
                "    `area_code`                 ,--STRING COMMENT '地区编码',\n" +
                "    `iso_code`                  ,--STRING COMMENT '旧版ISO-3166-2编码',\n" +
                "    `iso_3166_2`                ,--STRING COMMENT '新版ISO-3166-2编码',\n" +
                "    sum(sku_num) `order_count_30d`           ,--BIGINT COMMENT '最近30日下单次数',\n" +
                "    sum(split_original_amount)`order_original_amount_30d` ,--DECIMAL(16, 2) COMMENT '最近30日下单原始金额',\n" +
                "    sum(split_activity_amount)`activity_reduce_amount_30d`,--DECIMAL(16, 2) COMMENT '最近30日下单活动优惠金额',\n" +
                "    sum(split_coupon_amount) `coupon_reduce_amount_30d`  ,--DECIMAL(16, 2) COMMENT '最近30日下单优惠券优惠金额',\n" +
                "    sum(split_total_amount) `order_total_amount_30d`     --DECIMAL(16, 2) COMMENT '最近30日下单最终金额'\n" +
                "from\n" +
                "    (select\n" +
                "         province_id,\n" +
                "         k1,\n" +
                "         sku_num,\n" +
                "         split_original_amount,\n" +
                "         split_activity_amount,\n" +
                "         split_coupon_amount,\n" +
                "         split_total_amount\n" +
                "     from dwd.dwd_trade_order_detail_full\n" +
                "     where timestampdiff(day,to_timestamp(k1,'yyyy-MM-dd'),to_timestamp('2024-05-31','yyyy-MM-dd')) between 0 and 31) t1\n" +
                "left join\n" +
                "        (\n" +
                "            select\n" +
                "                id,\n" +
                "                province_name,\n" +
                "                area_code,\n" +
                "                iso_code,\n" +
                "                iso_3166_2\n" +
                "            from dim.dim_province_full\n" +
                "        ) t2\n" +
                "on t1.province_id = t2.id\n" +
                "group by province_id,k1,province_name,area_code,iso_code,iso_3166_2;";

        tableEnv.executeSql(insertSql);
        System.out.println("dws_trade_province_order_nd_full导入任务启动成功");
    }
}