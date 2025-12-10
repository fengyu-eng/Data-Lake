package com.fy.warehouse.dwd;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.concurrent.ExecutionException;

//互动域收藏商品事务事实表
public class dwd_interaction_favor_add_full {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
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
        tableEnv.useDatabase("dwd");

        //创建paimon表
        String paimonSql = "CREATE TABLE IF NOT EXISTS dwd.dwd_interaction_favor_add_full(\n" +
                "    `id`          BIGINT COMMENT '编号',\n" +
                "    `k1`          STRING COMMENT '分区字段',\n" +
                "    `user_id`     BIGINT COMMENT '用户id',\n" +
                "    `sku_id`      BIGINT COMMENT 'sku_id',\n" +
                "    `date_id`     STRING COMMENT '日期id',\n" +
                "    `create_time` timestamp(3) COMMENT '收藏时间',\n" +
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
                ");";

        tableEnv.executeSql(paimonSql);
        System.out.println("paimon表创建成功");

        //导入
        String insertSql = "insert into dwd.dwd_interaction_favor_add_full(\n" +
                "    id,\n" +
                "    k1,\n" +
                "    user_id,\n" +
                "    sku_id,\n" +
                "    date_id,\n" +
                "    create_time\n" +
                ")\n" +
                "select\n" +
                "    id,\n" +
                "    k1,\n" +
                "    user_id,\n" +
                "    sku_id,\n" +
                "    date_format(create_time,'yyyy-MM-dd') date_id,\n" +
                "    create_time\n" +
                "from ods.ods_favor_info_full;";
        TableResult tableResult = tableEnv.executeSql(insertSql);
        System.out.println("dwd_interaction_favor_add_full导入任务启动成功");
        tableResult.await();
    }
}
