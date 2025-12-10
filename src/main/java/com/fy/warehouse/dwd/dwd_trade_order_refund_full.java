package com.fy.warehouse.dwd;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.concurrent.ExecutionException;
//交易域退单事务事实表
public class dwd_trade_order_refund_full {
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
        String paimonSQL = "CREATE TABLE IF NOT EXISTS dwd.dwd_trade_order_refund_full(\n" +
                "    `id`                      BIGINT COMMENT '编号',\n" +
                "    `k1`                      STRING COMMENT '分区字段',\n" +
                "    `user_id`                 BIGINT COMMENT '用户ID',\n" +
                "    `order_id`                BIGINT COMMENT '订单ID',\n" +
                "    `sku_id`                  BIGINT COMMENT '商品ID',\n" +
                "    `province_id`             BIGINT COMMENT '地区ID',\n" +
                "    `date_id`                 STRING COMMENT '日期ID',\n" +
                "    `create_time`             TIMESTAMP(3) COMMENT '退单时间',\n" +
                "    `refund_type_code`        STRING COMMENT '退单类型编码',\n" +
                "    `refund_type_name`        STRING COMMENT '退单类型名称',\n" +
                "    `refund_reason_type_code` STRING COMMENT '退单原因类型编码',\n" +
                "    `refund_reason_type_name` STRING COMMENT '退单原因类型名称',\n" +
                "    `refund_reason_txt`       STRING COMMENT '退单原因描述',\n" +
                "    `refund_num`              BIGINT COMMENT '退单件数',\n" +
                "    `refund_amount`           DECIMAL(16, 2) COMMENT '退单金额',\n" +
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
        String insertSql = "INSERT INTO dwd.dwd_trade_order_refund_full(\n" +
                "    id,\n" +
                "    k1,\n" +
                "    user_id,\n" +
                "    order_id,\n" +
                "    sku_id,\n" +
                "    province_id,\n" +
                "    date_id,\n" +
                "    create_time,\n" +
                "    refund_type_code,\n" +
                "    refund_type_name,\n" +
                "    refund_reason_type_code,\n" +
                "    refund_reason_type_name,\n" +
                "    refund_reason_txt,\n" +
                "    refund_num,\n" +
                "    refund_amount)\n" +
                "select\n" +
                "    ri.id,\n" +
                "    k1,\n" +
                "    user_id,\n" +
                "    order_id,\n" +
                "    sku_id,\n" +
                "    province_id,\n" +
                "    date_format(create_time,'yyyy-MM-dd') date_id,\n" +
                "    create_time,\n" +
                "    refund_type,\n" +
                "    type_dic.dic_name,\n" +
                "    refund_reason_type,\n" +
                "    reason_dic.dic_name,\n" +
                "    refund_reason_txt,\n" +
                "    refund_num,\n" +
                "    refund_amount\n" +
                "from\n" +
                "    (\n" +
                "        select\n" +
                "            id,\n" +
                "            k1,\n" +
                "            user_id,\n" +
                "            order_id,\n" +
                "            sku_id,\n" +
                "            refund_type,\n" +
                "            refund_num,\n" +
                "            refund_amount,\n" +
                "            refund_reason_type,\n" +
                "            refund_reason_txt,\n" +
                "            create_time\n" +
                "        from ods.ods_order_refund_info_full\n" +
                "    )ri\n" +
                "        left join\n" +
                "    (\n" +
                "        select\n" +
                "            id,\n" +
                "            province_id\n" +
                "        from ods.ods_order_info_full\n" +
                "    )oi\n" +
                "    on ri.order_id=oi.id\n" +
                "        left join\n" +
                "    (\n" +
                "        select\n" +
                "            dic_code,\n" +
                "            dic_name\n" +
                "        from ods.ods_base_dic_full\n" +
                "        where parent_code = '15'\n" +
                "    )type_dic\n" +
                "    on ri.refund_type=type_dic.dic_code\n" +
                "        left join\n" +
                "    (\n" +
                "        select\n" +
                "            dic_code,\n" +
                "            dic_name\n" +
                "        from ods.ods_base_dic_full\n" +
                "        where  parent_code = '13'\n" +
                "    )reason_dic\n" +
                "    on ri.refund_reason_type=reason_dic.dic_code;";

        TableResult tableResult = tableEnv.executeSql(insertSql);
        System.out.println("dwd_trade_order_refund_full表任务启动成功");
        tableResult.await();
    }
}