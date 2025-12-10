package com.fy.warehouse.dws;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class dws_trade_user_sku_order_nd_full {
    public static void main(String[] args) {
        Configuration config = FlinkConfigUtil.getFlinkConfig();
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance()
                .withConfiguration(config)
                .build());

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
        tableEnv.useDatabase("dws");

        //创建piamon表
        String paimonSQL = "CREATE TABLE IF NOT EXISTS dws.dws_trade_user_sku_order_nd_full(\n" +
                "    `user_id`                    BIGINT COMMENT '用户id',\n" +
                "    `sku_id`                     BIGINT COMMENT 'sku_id',\n" +
                "    `k1`                         STRING COMMENT '分区字段',\n" +
                "    `sku_name`                   STRING COMMENT 'sku名称',\n" +
                "    `category1_id`               BIGINT COMMENT '一级分类id',\n" +
                "    `category1_name`             STRING COMMENT '一级分类名称',\n" +
                "    `category2_id`               BIGINT COMMENT '一级分类id',\n" +
                "    `category2_name`             STRING COMMENT '一级分类名称',\n" +
                "    `category3_id`               BIGINT COMMENT '一级分类id',\n" +
                "    `category3_name`             STRING COMMENT '一级分类名称',\n" +
                "    `tm_id`                      BIGINT COMMENT '品牌id',\n" +
                "    `tm_name`                    STRING COMMENT '品牌名称',\n" +
                "    `order_count_30d`            BIGINT COMMENT '最近30日下单次数',\n" +
                "    `order_num_30d`              BIGINT COMMENT '最近30日下单件数',\n" +
                "    `order_original_amount_30d`  DECIMAL(16, 2) COMMENT '最近30日下单原始金额',\n" +
                "    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '最近30日活动优惠金额',\n" +
                "    `coupon_reduce_amount_30d`   DECIMAL(16, 2) COMMENT '最近30日优惠券优惠金额',\n" +
                "    `order_total_amount_30d`     DECIMAL(16, 2) COMMENT '最近30日下单最终金额',\n" +
                "    PRIMARY KEY (`user_id`,`sku_id`,`k1` ) NOT ENFORCED\n" +
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
        tableEnv.executeSql(paimonSQL);
        System.out.println("paimon表创建成功");

        //导入
        String insertSQL = "insert into dws_trade_user_sku_order_nd_full select\n" +
                "    `user_id`                    ,--BIGINT COMMENT '用户id',\n" +
                "     `sku_id`                   ,--BIGINT COMMENT 'sku_id',\n" +
                "    `k1`                         ,--STRING COMMENT '分区字段',\n" +
                "    `sku_name`                   ,--STRING COMMENT 'sku名称',\n" +
                "    `category1_id`               ,--BIGINT COMMENT '一级分类id',\n" +
                "    `category1_name`             ,--STRING COMMENT '一级分类名称',\n" +
                "    `category2_id`               ,--BIGINT COMMENT '一级分类id',\n" +
                "    `category2_name`             ,--STRING COMMENT '一级分类名称',\n" +
                "    `category3_id`               ,--BIGINT COMMENT '一级分类id',\n" +
                "    `category3_name`             ,--STRING COMMENT '一级分类名称',\n" +
                "    `tm_id`                      ,--BIGINT COMMENT '品牌id',\n" +
                "    `tm_name`                    ,--STRING COMMENT '品牌名称',\n" +
                "    count(order_id)`order_count_30d`            ,--BIGINT COMMENT '最近30日下单次数',\n" +
                "    sum(sku_num)`order_num_30d`              ,--BIGINT COMMENT '最近30日下单件数',\n" +
                "    sum(split_original_amount)`order_original_amount_30d`  ,--DECIMAL(16, 2) COMMENT '最近30日下单原始金额',\n" +
                "    sum(split_activity_amount)`activity_reduce_amount_30d` ,--DECIMAL(16, 2) COMMENT '最近30日活动优惠金额',\n" +
                "    sum(split_coupon_amount)`coupon_reduce_amount_30d`   ,--DECIMAL(16, 2) COMMENT '最近30日优惠券优惠金额',\n" +
                "    sum(split_total_amount)`order_total_amount_30d`     --DECIMAL(16, 2) COMMENT '最近30日下单最终金额',\n" +
                "from(\n" +
                "    select\n" +
                "        user_id,\n" +
                "        sku_id,\n" +
                "        k1,\n" +
                "        order_id,\n" +
                "        sku_num,\n" +
                "        split_original_amount,\n" +
                "        split_activity_amount,\n" +
                "        split_coupon_amount,\n" +
                "        split_total_amount\n" +
                "    from dwd.dwd_trade_order_detail_full\n" +
                "    where timestampdiff(day,to_timestamp(k1,'yyyy-MM-dd'),to_timestamp('2024-05-01','yyyy-MM-dd')) between 0 and 30\n" +
                "    ) t\n" +
                "left join\n" +
                "(\n" +
                "    select\n" +
                "        id,\n" +
                "        sku_name,\n" +
                "        category1_id,\n" +
                "        category1_name,\n" +
                "        category2_id,\n" +
                "        category2_name,\n" +
                "        category3_id,\n" +
                "        category3_name,\n" +
                "        tm_id,\n" +
                "        tm_name\n" +
                "    from dim.dim_sku_full\n" +
                ")  t1\n" +
                "on t.sku_id = t1.id\n" +
                "group by `user_id`              ,\n" +
                "     `sku_id`                   ,\n" +
                "    `k1`                        ,\n" +
                "    `sku_name`                  ,\n" +
                "    `category1_id`              ,\n" +
                "    `category1_name`            ,\n" +
                "    `category2_id`              ,\n" +
                "    `category2_name`            ,\n" +
                "    `category3_id`              ,\n" +
                "    `category3_name`            ,\n" +
                "    `tm_id`                     ,\n" +
                "    `tm_name`                   ";
        tableEnv.executeSql(insertSQL);
        System.out.println("dws_trade_user_sku_order_nd_full导入任务启动成功");

    }
}
