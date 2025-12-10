package com.fy.warehouse.dwd;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.concurrent.ExecutionException;

//流量域页面浏览事务事实表
public class dwd_traffic_page_view_full {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // ========== 1. 核心配置补充（支持流式变更处理） ==========
        Configuration config = FlinkConfigUtil.getFlinkConfig();

        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance()
                .withConfiguration(config)
                .inStreamingMode() // 显式指定流式模式
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

        String paimonSQL = "CREATE TABLE IF NOT EXISTS dwd.dwd_traffic_page_view_full(\n" +
                "    `id`             STRING,\n" +
                "    `k1`             STRING COMMENT '分区字段',\n" +
                "    `province_id`    BIGINT COMMENT '省份id',\n" +
                "    `brand`          STRING COMMENT '手机品牌',\n" +
                "    `channel`        STRING COMMENT '渠道',\n" +
                "    `is_new`         STRING COMMENT '是否首次启动',\n" +
                "    `model`          STRING COMMENT '手机型号',\n" +
                "    `mid_id`         STRING COMMENT '设备id',\n" +
                "    `operate_system` STRING COMMENT '操作系统',\n" +
                "    `user_id`        STRING COMMENT '会员id',\n" +
                "    `version_code`   STRING COMMENT 'app版本号',\n" +
                "    `page_item`      STRING COMMENT '目标id ',\n" +
                "    `page_item_type` STRING COMMENT '目标类型',\n" +
                "    `last_page_id`   STRING COMMENT '上页类型',\n" +
                "    `page_id`        STRING COMMENT '页面ID ',\n" +
                "    `source_type`    STRING COMMENT '来源类型',\n" +
                "    `date_id`        STRING COMMENT '日期id',\n" +
                "    `view_time`      STRING COMMENT '跳入时间',\n" +
                "    `session_id`     STRING COMMENT '所属会话id',\n" +
                "    `during_time`    BIGINT COMMENT '持续时间毫秒',\n" +
                "    PRIMARY KEY (`id`,`k1` ) NOT ENFORCED\n" +
                ")   PARTITIONED BY (`k1` ) WITH (\n" +  // 修复：去掉多余的空格
                "    'connector' = 'paimon',\n" +
                "    'metastore.partitioned-table' = 'true',\n" +
                "    'file.format' = 'parquet',\n" +
                "    'write-buffer-size' = '512mb',\n" +
                "    'write-buffer-spillable' = 'true' ,\n" +
                "    'partition.expiration-time' = '1 d',\n" +
                "    'partition.expiration-check-interval' = '1 h',\n" +
                "    'partition.timestamp-formatter' = 'yyyy-MM-dd',\n" +
                "    'partition.timestamp-pattern' = '$k1'\n" +  // 修复：补充换行
                ");";

        tableEnv.executeSql(paimonSQL);
        System.out.println("paimon表创建成功");

        //设置表并行度
        tableEnv.executeSql("ALTER TABLE dwd.dwd_traffic_page_view_full SET (\n" +
                "    'sink.parallelism' = '10'\n" +
                "    );");


        String insertSql = "INSERT INTO dwd.dwd_traffic_page_view_full(\n" +
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
                "    date_id,\n" +
                "    view_time,\n" +
                "    session_id,\n" +
                "    during_time\n" +
                "    )\n" +
                "select\n" +
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
                "    page_last_page_id,\n" +
                "    page_page_id,\n" +
                "    page_source_type,\n" +
                "    DATE_FORMAT(FROM_UNIXTIME(cast(ts / 1000 as BIGINT)), 'yyyy-MM-dd') date_id,\n" +
                "    DATE_FORMAT(FROM_UNIXTIME(cast(ts / 1000 as BIGINT)), 'yyyy-MM-dd HH:mm:ss') view_time,\n" +
                "    concat(mid_id,'-',cast(max(session_start_point) as string)) session_id,\n" +
                "    page_during_time\n" +
                "from\n" +
                "    (\n" +
                "        select\n" +
                "            id,\n" +
                "            k1,\n" +
                "            common_ar area_code,\n" +
                "            common_ba brand,\n" +
                "            common_ch channel,\n" +
                "            common_is_new is_new,\n" +
                "            common_md model,\n" +
                "            common_mid mid_id,\n" +
                "            common_os operate_system,\n" +
                "            common_uid user_id,\n" +
                "            common_vc version_code,\n" +
                "            page_during_time,\n" +
                "            page_item ,\n" +
                "            page_item_type ,\n" +
                "            page_last_page_id,\n" +
                "            page_page_id,\n" +
                "            page_source_type,\n" +
                "            ts,\n" +
                "            ts session_start_point\n" +
                "        from ods.ods_log_inc\n" +
                "        where  page_during_time is not null\n" +
                "        and page_page_id is not null\n" +
                "    )log\n" +
                "    left join\n" +
                "    (\n" +
                "        select\n" +
                "            id province_id,\n" +
                "            area_code\n" +
                "        from ods.ods_base_province_full\n" +
                "    )bp\n" +
                "    on log.area_code=bp.area_code "+
                "GROUP BY \n" +
                "    id, k1, province_id, brand, channel, is_new, model, mid_id, operate_system, \n" +
                "    user_id, version_code, page_item, page_item_type, page_last_page_id, page_page_id, \n" +
                "    page_source_type, ts, page_during_time;";

        TableResult tableResult = tableEnv.executeSql(insertSql);
        System.out.println("dwd_traffic_page_view_full表任务启动成功");
        tableResult.await();
    }
}