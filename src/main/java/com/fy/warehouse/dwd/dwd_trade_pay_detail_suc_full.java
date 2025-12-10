package com.fy.warehouse.dwd;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.concurrent.ExecutionException;
//交易域支付成功事务事实表
public class dwd_trade_pay_detail_suc_full {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Configuration config = FlinkConfigUtil.getFlinkConfig();
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance()
                .withConfiguration(config)
                .build());

        //创建catalog
        String catalogSQL = "CREATE CATALOG paimon_hive WITH (\n" +
                "    'type' = 'paimon',\n" +
                "    'metastore' = 'hive',\n" +
                "    'uri' = 'thrift://192.168.10.102:9083',\n" +
                "    'warehouse' = 'hdfs://192.168.10.102/user/hive/warehouse'\n" +
                ");";
        tableEnv.executeSql(catalogSQL);
        System.out.println("catalog创建成功");

        tableEnv.useCatalog("paimon_hive");
        tableEnv.useDatabase("dwd");

        //创建paimon表
        String paimonSQL = "CREATE TABLE IF NOT EXISTS dwd.dwd_trade_pay_detail_suc_full(\n" +
                "    `id`                    BIGINT COMMENT '编号',\n" +
                "    `k1`                    STRING COMMENT '分区字段',\n" +
                "    `order_id`              BIGINT COMMENT '订单id',\n" +
                "    `user_id`               BIGINT COMMENT '用户id',\n" +
                "    `sku_id`                BIGINT COMMENT '商品id',\n" +
                "    `province_id`           BIGINT COMMENT '省份id',\n" +
                "    `activity_id`           BIGINT COMMENT '参与活动规则id',\n" +
                "    `activity_rule_id`      BIGINT COMMENT '参与活动规则id',\n" +
                "    `coupon_id`             BIGINT COMMENT '使用优惠券id',\n" +
                "    `payment_type_code`     STRING COMMENT '支付类型编码',\n" +
                "    `payment_type_name`     STRING COMMENT '支付类型名称',\n" +
                "    `date_id`               STRING COMMENT '支付日期id',\n" +
                "    `callback_time`         timestamp(3) COMMENT '支付成功时间',\n" +
                "    `source_id`             BIGINT COMMENT '来源编号',\n" +
                "    `source_type_code`      STRING COMMENT '来源类型编码',\n" +
                "    `source_type_name`      STRING COMMENT '来源类型名称',\n" +
                "    `sku_num`               BIGINT COMMENT '商品数量',\n" +
                "    `split_original_amount` DECIMAL(16, 2) COMMENT '应支付原始金额',\n" +
                "    `split_activity_amount` DECIMAL(16, 2) COMMENT '支付活动优惠分摊',\n" +
                "    `split_coupon_amount`   DECIMAL(16, 2) COMMENT '支付优惠券优惠分摊',\n" +
                "    `split_payment_amount`  DECIMAL(16, 2) COMMENT '支付金额',\n" +
                "    PRIMARY KEY (`id`,`k1` ) NOT ENFORCED\n" +
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

        //导入数据
        String insertSql = "INSERT INTO dwd.dwd_trade_pay_detail_suc_full(\n" +
                "    id,\n" +
                "    k1,\n" +
                "    order_id,\n" +
                "    user_id,\n" +
                "    sku_id,\n" +
                "    province_id,\n" +
                "    activity_id,\n" +
                "    activity_rule_id,\n" +
                "    coupon_id,\n" +
                "    payment_type_code,\n" +
                "    payment_type_name,\n" +
                "    date_id,\n" +
                "    callback_time,\n" +
                "    source_id,\n" +
                "    source_type_code,\n" +
                "    source_type_name,\n" +
                "    sku_num,\n" +
                "    split_original_amount,\n" +
                "    split_activity_amount,\n" +
                "    split_coupon_amount,\n" +
                "    split_payment_amount\n" +
                "    )\n" +
                "select\n" +
                "    od.id,\n" +
                "    k1,\n" +
                "    od.order_id,\n" +
                "    user_id,\n" +
                "    sku_id,\n" +
                "    province_id,\n" +
                "    activity_id,\n" +
                "    activity_rule_id,\n" +
                "    coupon_id,\n" +
                "    payment_type,\n" +
                "    pay_dic.dic_name,\n" +
                "    date_format(callback_time,'yyyy-MM-dd') date_id,\n" +
                "    callback_time,\n" +
                "    source_id,\n" +
                "    source_type,\n" +
                "    src_dic.dic_name,\n" +
                "    sku_num,\n" +
                "    split_original_amount,\n" +
                "    split_activity_amount,\n" +
                "    split_coupon_amount,\n" +
                "    split_total_amount\n" +
                "from\n" +
                "    (\n" +
                "        select\n" +
                "            id,\n" +
                "            k1,\n" +
                "            order_id,\n" +
                "            sku_id,\n" +
                "            source_id,\n" +
                "            source_type,\n" +
                "            sku_num,\n" +
                "            sku_num * order_price split_original_amount,\n" +
                "            split_total_amount,\n" +
                "            split_activity_amount,\n" +
                "            split_coupon_amount\n" +
                "        from ods.ods_order_detail_full\n" +
                "    ) od\n" +
                "        join\n" +
                "    (\n" +
                "        select\n" +
                "            user_id,\n" +
                "            order_id,\n" +
                "            payment_type,\n" +
                "            callback_time\n" +
                "        from ods.ods_payment_info_full\n" +
                "        where payment_status='1602'\n" +
                "    ) pi\n" +
                "    on od.order_id=pi.order_id\n" +
                "        left join\n" +
                "    (\n" +
                "        select\n" +
                "            id,\n" +
                "            province_id\n" +
                "        from ods.ods_order_info_full\n" +
                "    ) oi\n" +
                "    on od.order_id = oi.id\n" +
                "        left join\n" +
                "    (\n" +
                "        select\n" +
                "            order_detail_id,\n" +
                "            activity_id,\n" +
                "            activity_rule_id\n" +
                "        from ods.ods_order_detail_activity_full\n" +
                "    ) act\n" +
                "    on od.id = act.order_detail_id\n" +
                "        left join\n" +
                "    (\n" +
                "        select\n" +
                "            order_detail_id,\n" +
                "            coupon_id\n" +
                "        from ods.ods_order_detail_coupon_full\n" +
                "    ) cou\n" +
                "    on od.id = cou.order_detail_id\n" +
                "        left join\n" +
                "    (\n" +
                "        select\n" +
                "            dic_code,\n" +
                "            dic_name\n" +
                "        from ods.ods_base_dic_full\n" +
                "        where parent_code='11'\n" +
                "    ) pay_dic\n" +
                "    on pi.payment_type=pay_dic.dic_code\n" +
                "        left join\n" +
                "    (\n" +
                "        select\n" +
                "            dic_code,\n" +
                "            dic_name\n" +
                "        from ods.ods_base_dic_full\n" +
                "        where parent_code='24'\n" +
                "    )src_dic\n" +
                "    on od.source_type=src_dic.dic_code;";

        TableResult tableResult = tableEnv.executeSql(insertSql);
        System.out.println("dwd_trade_pay_detail_suc_full表任务启动成功");
        tableResult.await();
    }
}