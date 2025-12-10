package com.fy.warehouse.dwd;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.concurrent.ExecutionException;
//交易域退款支付成功事务事实表
public class dwd_trade_refund_pay_suc_full {
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
        String paimonSQL = "CREATE TABLE IF NOT EXISTS dwd.dwd_trade_refund_pay_suc_full(\n" +
                "    `id`                BIGINT COMMENT '编号',\n" +
                "    `k1`                STRING COMMENT '分区字段',\n" +
                "    `user_id`           BIGINT COMMENT '用户ID',\n" +
                "    `order_id`          BIGINT COMMENT '订单编号',\n" +
                "    `sku_id`            BIGINT COMMENT 'SKU编号',\n" +
                "    `province_id`       BIGINT COMMENT '地区ID',\n" +
                "    `payment_type_code` STRING COMMENT '支付类型编码',\n" +
                "    `payment_type_name` STRING COMMENT '支付类型名称',\n" +
                "    `date_id`           STRING COMMENT '日期ID',\n" +
                "    `callback_time`     TIMESTAMP(3) COMMENT '支付成功时间',\n" +
                "    `refund_num`        DECIMAL(16, 2) COMMENT '退款件数',\n" +
                "    `refund_amount`     DECIMAL(16, 2) COMMENT '退款金额',\n" +
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
        String insertSql = "INSERT INTO dwd.dwd_trade_refund_pay_suc_full(\n" +
                "    id,\n" +
                "    k1,\n" +
                "    user_id,\n" +
                "    order_id,\n" +
                "    sku_id,\n" +
                "    province_id,\n" +
                "    payment_type_code,\n" +
                "    payment_type_name,\n" +
                "    date_id,\n" +
                "    callback_time,\n" +
                "    refund_num,\n" +
                "    refund_amount\n" +
                "    )\n" +
                "select\n" +
                "    rp.id,\n" +
                "    k1,\n" +
                "    user_id,\n" +
                "    rp.order_id,\n" +
                "    rp.sku_id,\n" +
                "    province_id,\n" +
                "    payment_type,\n" +
                "    dic_name,\n" +
                "    date_format(callback_time,'yyyy-MM-dd') date_id,\n" +
                "    callback_time,\n" +
                "    refund_num,\n" +
                "    total_amount\n" +
                "from\n" +
                "    (\n" +
                "        select\n" +
                "            id,\n" +
                "            k1,\n" +
                "            order_id,\n" +
                "            sku_id,\n" +
                "            payment_type,\n" +
                "            callback_time,\n" +
                "            total_amount\n" +
                "        from ods.ods_refund_payment_full\n" +
                "        -- where refund_status='1602'\n" +
                "    )rp\n" +
                "        left join\n" +
                "    (\n" +
                "        select\n" +
                "            id,\n" +
                "            user_id,\n" +
                "            province_id\n" +
                "        from ods.ods_order_info_full\n" +
                "    )oi\n" +
                "    on rp.order_id=oi.id\n" +
                "        left join\n" +
                "    (\n" +
                "        select\n" +
                "            order_id,\n" +
                "            sku_id,\n" +
                "            refund_num\n" +
                "        from ods.ods_order_refund_info_full\n" +
                "    )ri\n" +
                "    on rp.order_id=ri.order_id\n" +
                "        and rp.sku_id=ri.sku_id\n" +
                "        left join\n" +
                "    (\n" +
                "        select\n" +
                "            dic_code,\n" +
                "            dic_name\n" +
                "        from ods.ods_base_dic_full\n" +
                "        where parent_code='11'\n" +
                "    )dic\n" +
                "    on rp.payment_type=dic.dic_code;";

        TableResult tableResult = tableEnv.executeSql(insertSql);
        System.out.println("dwd_trade_refund_pay_suc_full表任务启动成功");
        tableResult.await();
    }
}