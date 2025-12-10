package com.fy.warehouse.dws;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
//用户域累积登录至今表
public class dws_user_login_td_full {
    public static void main(String[] args) {
        Configuration config = FlinkConfigUtil.getFlinkConfig();
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance()
                .withConfiguration(config).build());

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
        tableEnv.useDatabase("dws");

        //创建piamon表
        String paimonSQL = "CREATE TABLE IF NOT EXISTS dws.dws_user_login_td_full(\n" +
                "    `user_id`         BIGINT COMMENT '用户id',\n" +
                "    `k1`              STRING COMMENT '分区字段',\n" +
                "    `login_date_last` STRING COMMENT '末次登录日期',\n" +
                "    `login_count_td`  BIGINT COMMENT '累计登录次数',\n" +
                "    PRIMARY KEY (`user_id`,`k1` ) NOT ENFORCED\n" +
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
        String insertSQL = "INSERT INTO dws.dws_user_login_td_full(user_id, k1, login_date_last, login_count_td)\n" +
                "select\n" +
                "    u.id,\n" +
                "    u.k1,\n" +
                "    login_date_last,\n" +
                "    login_count_td\n" +
                "from\n" +
                "    (\n" +
                "        select\n" +
                "            id,\n" +
                "            k1,\n" +
                "            create_time\n" +
                "        from dim.dim_user_zip_full\n" +
                "    )u\n" +
                "        left join\n" +
                "    (\n" +
                "        select\n" +
                "            user_id,\n" +
                "            max(k1) login_date_last,\n" +
                "            count(*) login_count_td\n" +
                "        from dwd.dwd_user_login_full\n" +
                "        group by user_id\n" +
                "    )l\n" +
                "    on cast(u.id as STRING)=l.user_id;";

        tableEnv.executeSql(insertSQL);
        System.out.println("dws_user_login_td_full导入任务启动成功");
    }
}
