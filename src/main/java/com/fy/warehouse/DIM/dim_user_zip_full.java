package com.fy.warehouse.DIM;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.concurrent.ExecutionException;

public class dim_user_zip_full {
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
        tableEnv.useDatabase("dim");

        //创建paimon表
        String paimonSQL = "CREATE TABLE IF NOT EXISTS dim.dim_user_zip_full(\n" +
                "    `id`           BIGINT COMMENT '用户id',\n" +
                "    `k1`           STRING COMMENT '分区字段',\n" +
                "    `login_name`   STRING COMMENT '用户名称',\n" +
                "    `nick_name`    STRING COMMENT '用户昵称',\n" +
                "    `name`         STRING COMMENT '用户姓名',\n" +
                "    `phone_num`    STRING COMMENT '手机号码',\n" +
                "    `email`        STRING COMMENT '邮箱',\n" +
                "    `user_level`   STRING COMMENT '用户等级',\n" +
                "    `birthday`     STRING COMMENT '生日',\n" +
                "    `gender`       STRING COMMENT '性别',\n" +
                "    `create_time`  TIMESTAMP(3) COMMENT '创建时间',\n" +
                "    `operate_time` TIMESTAMP(3) COMMENT '操作时间',\n" +
                "    `start_date`   CHAR(10)  COMMENT '开始日期',\n" +
                "    `end_date`     CHAR(10)  COMMENT '结束日期',\n" +
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
                "    );\n";

        tableEnv.executeSql(paimonSQL);
        System.out.println("paimon表创建成功");

        //导入
        String insertSQL = "insert into dim.dim_user_zip_full(id, k1, login_name, nick_name, name, phone_num, email, user_level, birthday, gender, create_time, operate_time, start_date, end_date)\n" +
                "select\n" +
                "    id,\n" +
                "    k1,\n" +
                "    login_name,\n" +
                "    nick_name,\n" +
                "    md5(name),\n" +
                "    md5(phone_num),\n" +
                "    md5(email),\n" +
                "    user_level,\n" +
                "    birthday,\n" +
                "    gender,\n" +
                "    create_time,\n" +
                "    operate_time,\n" +
                "    CAST(DATE_FORMAT(operate_time, 'yyyy-MM-dd') AS CHAR(10)) start_date,\n" +
                "    '9999-12-31' end_date\n" +
                "from (select\n" +
                "    *\n" +
                "from (select *,\n" +
                "             row_number() over (partition by id order by operate_time desc) rk\n" +
                "      from ods.ods_user_info_full) t\n" +
                "where rk = 1);";
        TableResult tableResult = tableEnv.executeSql(insertSQL);
        System.out.println("dim_user_zip导入任务启动成功");
        tableResult.await();
    }
}
