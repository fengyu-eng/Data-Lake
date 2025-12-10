package com.fy.warehouse.dwd;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.concurrent.ExecutionException;
//流量域错误事务事实表
public class dwd_traffic_error_full {
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
        String paimonSQL = "CREATE TABLE IF NOT EXISTS dwd.dwd_traffic_error_full(\n" +
                "    `id`              STRING,\n" +
                "    `k1`              STRING COMMENT '分区字段',\n" +
                "    `province_id`     BIGINT COMMENT '地区编码',\n" +
                "    `brand`           STRING COMMENT '手机品牌',\n" +
                "    `channel`         STRING COMMENT '渠道',\n" +
                "    `is_new`          STRING COMMENT '是否首次启动',\n" +
                "    `model`           STRING COMMENT '手机型号',\n" +
                "    `mid_id`          STRING COMMENT '设备id',\n" +
                "    `operate_system`  STRING COMMENT '操作系统',\n" +
                "    `user_id`         STRING COMMENT '会员id',\n" +
                "    `version_code`    STRING COMMENT 'app版本号',\n" +
                "    `page_item`       STRING COMMENT '目标id ',\n" +
                "    `page_item_type`  STRING COMMENT '目标类型',\n" +
                "    `last_page_id`    STRING COMMENT '上页类型',\n" +
                "    `page_id`         STRING COMMENT '页面ID ',\n" +
                "    `source_type`     STRING COMMENT '来源类型',\n" +
                "    `entry`           STRING COMMENT 'icon手机图标  notice 通知',\n" +
                "    `loading_time`    STRING COMMENT '启动加载时间',\n" +
                "    `open_ad_id`      STRING COMMENT '广告页ID ',\n" +
                "    `open_ad_ms`      STRING COMMENT '广告总共播放时间',\n" +
                "    `open_ad_skip_ms` STRING COMMENT '用户跳过广告时点',\n" +
                "    `action_id`        STRING COMMENT '动作id',\n" +
                "    `action_item`      STRING COMMENT '目标id ',\n" +
                "    `action_item_type` STRING COMMENT '目标类型',\n" +
                "    `action_time`      STRING COMMENT '动作发生时间',\n" +
                "    `display_type`      STRING COMMENT '曝光类型',\n" +
                "    `display_item`      STRING COMMENT '曝光对象id ',\n" +
                "    `display_item_type` STRING COMMENT 'app版本号',\n" +
                "    `display_order`     BIGINT COMMENT '曝光顺序',\n" +
                "    `display_pos_id`    BIGINT COMMENT '曝光位置',\n" +
                "    `date_id`         STRING COMMENT '日期id',\n" +
                "    `error_time`      STRING COMMENT '错误时间',\n" +
                "    `error_code`      BIGINT COMMENT '错误码',\n" +
                "    `error_msg`       STRING COMMENT '错误信息',\n" +
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

        //创建临时UDF函数
        tableEnv.executeSql("CREATE TEMPORARY FUNCTION json_actions_array_parser AS 'org.bigdatatechcir.warehouse.flink.udf.JsonActionsArrayParser';");
        tableEnv.executeSql("CREATE TEMPORARY FUNCTION json_displays_array_parser AS 'org.bigdatatechcir.warehouse.flink.udf.JsonDisplaysArrayParser';");

        //导入数据
        String insertSql = "INSERT INTO dwd.dwd_traffic_error_full(\n" +
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
                "    page_item,\n" +
                "    page_item_type,\n" +
                "    last_page_id,\n" +
                "    page_id,\n" +
                "    source_type,\n" +
                "    entry,\n" +
                "    loading_time,\n" +
                "    open_ad_id,\n" +
                "    open_ad_ms,\n" +
                "    open_ad_skip_ms,\n" +
                "    action_id,\n" +
                "    action_item,\n" +
                "    action_item_type,\n" +
                "    action_time,\n" +
                "    display_type,\n" +
                "    display_item,\n" +
                "    display_item_type,\n" +
                "    display_order,\n" +
                "    display_pos_id,\n" +
                "    date_id,\n" +
                "    error_time,\n" +
                "    error_code,\n" +
                "    error_msg\n" +
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
                "    page_item,\n" +
                "    page_item_type,\n" +
                "    page_last_page_id,\n" +
                "    page_page_id,\n" +
                "    page_source_type,\n" +
                "    start_entry,\n" +
                "    start_loading_time,\n" +
                "    start_open_ad_id,\n" +
                "    start_open_ad_ms,\n" +
                "    start_open_ad_skip_ms,\n" +
                "    action_id,\n" +
                "    action_item,\n" +
                "    action_item_type,\n" +
                "    DATE_FORMAT(FROM_UNIXTIME(cast(ts / 1000 as BIGINT)), 'yyyy-MM-dd') action_time,\n" +
                "    display_type,\n" +
                "    display_item,\n" +
                "    display_item_type,\n" +
                "    display_order,\n" +
                "    display_pos_id,\n" +
                "    DATE_FORMAT(FROM_UNIXTIME(cast(ts / 1000 as BIGINT)), 'yyyy-MM-dd') date_id,\n" +
                "    DATE_FORMAT(FROM_UNIXTIME(cast(ts / 1000 as BIGINT)), 'yyyy-MM-dd HH:mm:ss') error_time,\n" +
                "    err_error_code,\n" +
                "    error_msg\n" +
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
                "            page_during_time,\n" +
                "            page_item page_item,\n" +
                "            page_item_type page_item_type,\n" +
                "            page_last_page_id,\n" +
                "            page_page_id,\n" +
                "            page_source_type,\n" +
                "            start_entry,\n" +
                "            start_loading_time,\n" +
                "            start_open_ad_id,\n" +
                "            start_open_ad_ms,\n" +
                "            start_open_ad_skip_ms,\n" +
                "            json_actions_array_parser(`actions`).`action_id` as action_id,\n" +
                "            json_actions_array_parser(`actions`).`item` as action_item,\n" +
                "            json_actions_array_parser(`actions`).`item_type` as action_item_type,\n" +
                "            json_actions_array_parser(`actions`).`ts` as ts,\n" +
                "            json_displays_array_parser(`displays`).`display_type` as display_type,\n" +
                "            json_displays_array_parser(`displays`).`item` as display_item,\n" +
                "            json_displays_array_parser(`displays`).`item_type` as display_item_type,\n" +
                "            json_displays_array_parser(`displays`).`order` as display_order,\n" +
                "            json_displays_array_parser(`displays`).`pos_id` as display_pos_id,\n" +
                "            err_error_code,\n" +
                "            err_msg error_msg\n" +
                "        from ods.ods_log_inc\n" +
                "        where  err_error_code is not null\n" +
                "    )log\n" +
                "        join\n" +
                "    (\n" +
                "        select\n" +
                "            id province_id,\n" +
                "            area_code\n" +
                "        from ods.ods_base_province_full\n" +
                "    )bp\n" +
                "    on log.area_code=bp.area_code;";

        TableResult tableResult = tableEnv.executeSql(insertSql);
        System.out.println("dwd_traffic_error_full表任务启动成功");
        tableResult.await();
    }
}