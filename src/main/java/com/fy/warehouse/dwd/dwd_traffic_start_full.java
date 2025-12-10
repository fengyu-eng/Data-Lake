package com.fy.warehouse.dwd;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.concurrent.ExecutionException;
//流量域启动事务事实表
public class dwd_traffic_start_full {
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
        String paimonSQL = "CREATE TABLE IF NOT EXISTS dwd.dwd_traffic_start_full(\n" +
                "    `id`              STRING,\n" +
                "    `k1`              STRING COMMENT '分区字段',\n" +
                "    `province_id`     BIGINT COMMENT '省份id',\n" +
                "    `brand`           STRING COMMENT '手机品牌',\n" +
                "    `channel`         STRING COMMENT '渠道',\n" +
                "    `is_new`          STRING COMMENT '是否首次启动',\n" +
                "    `model`           STRING COMMENT '手机型号',\n" +
                "    `mid_id`          STRING COMMENT '设备id',\n" +
                "    `operate_system`  STRING COMMENT '操作系统',\n" +
                "    `user_id`         STRING COMMENT '会员id',\n" +
                "    `version_code`    STRING COMMENT 'app版本号',\n" +
                "    `entry`           STRING COMMENT 'icon手机图标 notice 通知',\n" +
                "    `open_ad_id`      STRING COMMENT '广告页ID ',\n" +
                "    `date_id`         STRING COMMENT '日期id',\n" +
                "    `start_time`      STRING COMMENT '启动时间',\n" +
                "    `loading_time_ms` STRING COMMENT '启动加载时间',\n" +
                "    `open_ad_ms`      STRING COMMENT '广告总共播放时间',\n" +
                "    `open_ad_skip_ms` STRING COMMENT '用户跳过广告时点',\n" +
                "    PRIMARY KEY (`id`,`k1` ) NOT ENFORCED\n" +
                "    )   PARTITIONED BY (`k1` ) WITH (\n" +
                "   'connector' = 'paimon',\n" +
                "   'metastore.partitioned-table' = 'true',\n" +
                "   'file.format' = 'parquet',\n" +
                "   'write-buffer-size' = '512mb',\n" +
                "   'write-buffer-spillable' = 'true' ,\n" +
                "   'partition.expiration-time' = '1 d',\n" +
                "   'partition.expiration-check-interval' = '1 h',\n" +
                "   'partition.timestamp-formatter' = 'yyyy-MM-dd',\n" +
                "   'partition.timestamp-pattern' = '$k1'\n" +
                "   );";

        tableEnv.executeSql(paimonSQL);
        System.out.println("paimon表创建成功");

        //导入数据
        String insertSql = "INSERT INTO dwd.dwd_traffic_start_full(\n" +
                "    id,\n" +
                "    k1,\n" +
                "    province_id,\n" +
                "    brand,\n" +
                "    channel,\n" +
                "    is_new,\n" +
                "    model,\n" +
                "    mid_id,\n" +
                "    operate_system,\n" +
                "    user_id,\n" +
                "    version_code,\n" +
                "    entry,\n" +
                "    open_ad_id,\n" +
                "    date_id,\n" +
                "    start_time,\n" +
                "    loading_time_ms,\n" +
                "    open_ad_ms,\n" +
                "    open_ad_skip_ms\n" +
                "    )\n" +
                "select\n" +
                "    id,\n" +
                "    k1,\n" +
                "    province_id,\n" +
                "    brand,\n" +
                "    channel,\n" +
                "    common_is_new,\n" +
                "    model,\n" +
                "    mid_id,\n" +
                "    operate_system,\n" +
                "    user_id,\n" +
                "    version_code,\n" +
                "    start_entry,\n" +
                "    start_open_ad_id,\n" +
                "    DATE_FORMAT(FROM_UNIXTIME(cast(ts / 1000 as BIGINT)), 'yyyy-MM-dd') date_id,\n" +
                "    DATE_FORMAT(FROM_UNIXTIME(cast(ts / 1000 as BIGINT)), 'yyyy-MM-dd HH:mm:ss') action_time,\n" +
                "    start_loading_time,\n" +
                "    start_open_ad_ms,\n" +
                "    start_open_ad_skip_ms\n" +
                "from\n" +
                "    (\n" +
                "        select\n" +
                "            id,\n" +
                "            k1,\n" +
                "            common_ar area_code,\n" +
                "            common_ba brand,\n" +
                "            common_ch channel,\n" +
                "            common_is_new,\n" +
                "            common_md model,\n" +
                "            common_mid mid_id,\n" +
                "            common_os operate_system,\n" +
                "            common_uid user_id,\n" +
                "            common_vc version_code,\n" +
                "            start_entry,\n" +
                "            start_loading_time,\n" +
                "            start_open_ad_id,\n" +
                "            start_open_ad_ms,\n" +
                "            start_open_ad_skip_ms,\n" +
                "            ts\n" +
                "        from ods.ods_log_inc\n" +
                "        where start_entry is not null\n" +
                "    )log\n" +
                "        left join\n" +
                "    (\n" +
                "        select\n" +
                "            id province_id,\n" +
                "            area_code\n" +
                "        from ods.ods_base_province_full\n" +
                "    )bp\n" +
                "    on log.area_code=bp.area_code;";

        TableResult tableResult = tableEnv.executeSql(insertSql);
        System.out.println("dwd_traffic_start_full表任务启动成功");
        tableResult.await();
    }
}