package com.fy.warehouse.dwd;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.concurrent.ExecutionException;
//交易域加购事务事实表
public class dwd_trade_cart_add_full {
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
        String paimonSQL = "CREATE TABLE IF NOT EXISTS dwd.dwd_trade_cart_add_full(\n" +
                "    `id`               BIGINT COMMENT '编号',\n" +
                "    `k1`               STRING COMMENT '分区字段',\n" +
                "    `user_id`          STRING COMMENT '用户id',\n" +
                "    `sku_id`           BIGINT COMMENT '商品id',\n" +
                "    `date_id`          STRING COMMENT '时间id',\n" +
                "    `create_time`      timestamp(3) COMMENT '加购时间',\n" +
                "    `source_id`        BIGINT COMMENT '来源类型ID',\n" +
                "    `source_type_code` STRING COMMENT '来源类型编码',\n" +
                "    `source_type_name` STRING COMMENT '来源类型名称',\n" +
                "    `sku_num`          BIGINT COMMENT '加购物车件数',\n" +
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
        String insertSql = "INSERT INTO dwd.dwd_trade_cart_add_full(\n" +
                "    id,\n" +
                "    k1,\n" +
                "    user_id,\n" +
                "    sku_id,\n" +
                "    date_id,\n" +
                "    create_time,\n" +
                "    source_id,\n" +
                "    source_type_code,\n" +
                "    source_type_name,\n" +
                "    sku_num\n" +
                "    )\n" +
                "select\n" +
                "    id,\n" +
                "    date_format(create_time,'yyyy-MM-dd') as k1,\n" +
                "    user_id,\n" +
                "    sku_id,\n" +
                "    date_format(create_time,'yyyy-MM-dd') date_id,\n" +
                "    create_time,\n" +
                "    source_id,\n" +
                "    source_type,\n" +
                "    dic.dic_name,\n" +
                "    sku_num\n" +
                "from\n" +
                "    (\n" +
                "        select\n" +
                "            id,\n" +
                "            user_id,\n" +
                "            sku_id,\n" +
                "            create_time,\n" +
                "            source_id,\n" +
                "            source_type,\n" +
                "            sku_num\n" +
                "        from ods.ods_cart_info_full\n" +
                "    )ci\n" +
                "        left join\n" +
                "    (\n" +
                "        select\n" +
                "            dic_code,\n" +
                "            dic_name\n" +
                "        from ods.ods_base_dic_full\n" +
                "        where parent_code='24'\n" +
                "    )dic\n" +
                "    on ci.source_type=dic.dic_code;";

        TableResult tableResult = tableEnv.executeSql(insertSql);
        System.out.println("dwd_trade_cart_add_full表任务启动成功");
        tableResult.await();
    }
}