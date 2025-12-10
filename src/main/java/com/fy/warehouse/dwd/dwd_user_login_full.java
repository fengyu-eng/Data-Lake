package com.fy.warehouse.dwd;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.concurrent.ExecutionException;
//用户域用户登录事务事实表
public class dwd_user_login_full {
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
        String paimonSQL = "CREATE TABLE IF NOT EXISTS dwd.dwd_user_login_full(\n" +
                "    `k1`             STRING COMMENT '分区字段',\n" +
                "    `user_id`        STRING COMMENT '用户ID',\n" +
                "    `date_id`        STRING COMMENT '日期ID',\n" +
                "    `login_time`     STRING COMMENT '登录时间',\n" +
                "    `channel`        STRING COMMENT '应用下载渠道',\n" +
                "    `province_id`    BIGINT COMMENT '省份id',\n" +
                "    `version_code`   STRING COMMENT '应用版本',\n" +
                "    `mid_id`         STRING COMMENT '设备id',\n" +
                "    `brand`          STRING COMMENT '设备品牌',\n" +
                "    `model`          STRING COMMENT '设备型号',\n" +
                "    `operate_system` STRING COMMENT '设备操作系统',\n" +
                "    PRIMARY KEY (`k1`,`user_id`,`date_id` ) NOT ENFORCED\n" +
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

        //设置表并行度
        tableEnv.executeSql("ALTER TABLE dwd.dwd_user_login_full SET (\n" +
                "    'sink.parallelism' = '10'\n" +
                "    );");

        //导入数据
        String insertSql = "INSERT INTO dwd.dwd_user_login_full(\n" +
                "    k1,\n" +
                "    user_id,\n" +
                "    date_id,\n" +
                "    login_time,\n" +
                "    channel,\n" +
                "    province_id,\n" +
                "    version_code,\n" +
                "    mid_id,\n" +
                "    brand,\n" +
                "    model,\n" +
                "    operate_system\n" +
                "    )\n" +
                "select\n" +
                "    k1,\n" +
                "    user_id,\n" +
                "    DATE_FORMAT(FROM_UNIXTIME(cast(ts / 1000 as BIGINT)), 'yyyy-MM-dd') date_id,\n" +
                "    DATE_FORMAT(FROM_UNIXTIME(cast(ts / 1000 as BIGINT)), 'yyyy-MM-dd HH:mm:ss') login_time,\n" +
                "    channel,\n" +
                "    province_id,\n" +
                "    version_code,\n" +
                "    mid_id,\n" +
                "    brand,\n" +
                "    model,\n" +
                "    operate_system\n" +
                "from\n" +
                "    (\n" +
                "        select\n" +
                "            user_id,\n" +
                "            k1,\n" +
                "            channel,\n" +
                "            area_code,\n" +
                "            version_code,\n" +
                "            mid_id,\n" +
                "            brand,\n" +
                "            model,\n" +
                "            operate_system,\n" +
                "            ts\n" +
                "        from\n" +
                "            (\n" +
                "                select\n" +
                "                    user_id,\n" +
                "                    k1,\n" +
                "                    channel,\n" +
                "                    area_code,\n" +
                "                    version_code,\n" +
                "                    mid_id,\n" +
                "                    brand,\n" +
                "                    model,\n" +
                "                    operate_system,\n" +
                "                    ts,\n" +
                "                    row_number() over (partition by session_id order by ts) rn\n" +
                "                from\n" +
                "                    (\n" +
                "                        select\n" +
                "                            user_id,\n" +
                "                            k1,\n" +
                "                            channel,\n" +
                "                            area_code,\n" +
                "                            version_code,\n" +
                "                            mid_id,\n" +
                "                            brand,\n" +
                "                            model,\n" +
                "                            operate_system,\n" +
                "                            ts,\n" +
                "                            concat(mid_id,'-',cast(session_start_point as string)) session_id\n" +
                "                        from\n" +
                "                            (\n" +
                "                                select\n" +
                "                                    common_uid user_id,\n" +
                "                                    k1,\n" +
                "                                    common_ch channel,\n" +
                "                                    common_ar area_code,\n" +
                "                                    common_vc version_code,\n" +
                "                                    common_mid mid_id,\n" +
                "                                    common_ba brand,\n" +
                "                                    common_md model,\n" +
                "                                    common_os operate_system,\n" +
                "                                    ts,\n" +
                "                                    ts session_start_point\n" +
                "                                from ods.ods_log_inc\n" +
                "                                where  page_last_page_id is not null\n" +
                "                            )t1\n" +
                "                    )t2\n" +
                "                where user_id is not null\n" +
                "            )t3\n" +
                "        where rn=1\n" +
                "    )t4\n" +
                "        left join\n" +
                "    (\n" +
                "        select\n" +
                "            id province_id,\n" +
                "            area_code\n" +
                "        from ods.ods_base_province_full\n" +
                "    )bp\n" +
                "    on t4.area_code=bp.area_code;";

        TableResult tableResult = tableEnv.executeSql(insertSql);
        System.out.println("dwd_user_login_full表任务启动成功");
        tableResult.await();
    }
}