package com.fy.warehouse.dwd;

//工具域优惠卷使用(支付)事务事实表

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.concurrent.ExecutionException;

public class dwd_tool_coupon_pay_full {
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
        String paimonSQL = "CREATE TABLE IF NOT EXISTS dwd.dwd_tool_coupon_pay_full(\n" +
                "    `id`           BIGINT COMMENT '编号',\n" +
                "    `k1`           STRING COMMENT '分区字段',\n" +
                "    `coupon_id`    BIGINT COMMENT '优惠券ID',\n" +
                "    `user_id`      BIGINT COMMENT 'user_id',\n" +
                "    `order_id`     BIGINT COMMENT 'order_id',\n" +
                "    `date_id`      STRING COMMENT '日期ID',\n" +
                "    `payment_time` TIMESTAMP(3) COMMENT '使用下单时间',\n" +
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
        String insertSql = "insert into dwd.dwd_tool_coupon_pay_full(\n" +
                "    id,\n" +
                "    k1,\n" +
                "    coupon_id,\n" +
                "    user_id,\n" +
                "    order_id,\n" +
                "    date_id,\n" +
                "    payment_time\n" +
                ")\n" +
                "select\n" +
                "    id,\n" +
                "    k1,\n" +
                "    coupon_id,\n" +
                "    user_id,\n" +
                "    order_id,\n" +
                "    date_format(used_time,'yyyy-MM-dd') date_id,\n" +
                "    used_time\n" +
                "from ods.ods_coupon_use_full\n" +
                "where used_time is not null;";

        TableResult tableResult = tableEnv.executeSql(insertSql);
        System.out.println("dwd_tool_coupon_pay_full表任务启动成功");
        tableResult.await();
    }
}
