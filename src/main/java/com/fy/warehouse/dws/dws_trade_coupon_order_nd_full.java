package com.fy.warehouse.dws;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

//使用优惠卷下单金额30日汇总表
public class dws_trade_coupon_order_nd_full {
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

        //创建paimon表
        String paimonSql = "CREATE TABLE IF NOT EXISTS dws.dws_trade_coupon_order_nd_full(\n" +
                "    `coupon_id`                BIGINT COMMENT '优惠券id',\n" +
                "    `k1`                       STRING COMMENT '分区字段',\n" +
                "    `coupon_name`              STRING COMMENT '优惠券名称',\n" +
                "    `coupon_type_code`         STRING COMMENT '优惠券类型id',\n" +
                "    `coupon_type_name`         STRING COMMENT '优惠券类型名称',\n" +
                "    `coupon_rule`              STRING COMMENT '优惠券规则',\n" +
                "    `start_date`               STRING COMMENT '发布日期',\n" +
                "    `original_amount_30d`      DECIMAL(16, 2) COMMENT '使用下单原始金额',\n" +
                "    `coupon_reduce_amount_30d` DECIMAL(16, 2) COMMENT '使用下单优惠金额',\n" +
                "    PRIMARY KEY (`coupon_id`,`k1` ) NOT ENFORCED\n" +
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

        //导入
        String insertSql = "INSERT INTO dws.dws_trade_coupon_order_nd_full(coupon_id, k1, coupon_name, coupon_type_code, coupon_type_name, coupon_rule, start_date, original_amount_30d, coupon_reduce_amount_30d)\n" +
                "select\n" +
                "    id,\n" +
                "    od.k1,\n" +
                "    coupon_name,\n" +
                "    coupon_type_code,\n" +
                "    coupon_type_name,\n" +
                "    benefit_rule,\n" +
                "    start_date,\n" +
                "    sum(split_original_amount),\n" +
                "    sum(split_coupon_amount)\n" +
                "from\n" +
                "    (\n" +
                "        select\n" +
                "            id,\n" +
                "            coupon_name,\n" +
                "            coupon_type_code,\n" +
                "            coupon_type_name,\n" +
                "            benefit_rule,\n" +
                "            date_format(start_time,'yyyy-MM-dd') start_date\n" +
                "        from dim.dim_coupon_full\n" +
                "    )cou\n" +
                "        left join\n" +
                "    (\n" +
                "        select\n" +
                "            coupon_id,\n" +
                "            k1,\n" +
                "            order_id,\n" +
                "            split_original_amount,\n" +
                "            split_coupon_amount\n" +
                "        from dwd.dwd_trade_order_detail_full\n" +
                "        where coupon_id is not null and timestampdiff(day,to_timestamp(k1,'yyyy-MM-dd'),to_timestamp('2024-05-31','yyyy-MM-dd')) between 0 and 30\n" +
                "    )od\n" +
                "    on cou.id=od.coupon_id\n" +
                "group by id,od.k1,coupon_name,coupon_type_code,coupon_type_name,benefit_rule,start_date;";

        tableEnv.executeSql(insertSql);
        System.out.println("dws_trade_coupon_order_nd_full导入任务启动成功");

    }
}
