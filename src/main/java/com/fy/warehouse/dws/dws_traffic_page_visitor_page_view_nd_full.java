package com.fy.warehouse.dws;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class dws_traffic_page_visitor_page_view_nd_full {
    public static void main(String[] args) {
        // 获取Flink配置
        Configuration config = FlinkConfigUtil.getFlinkConfig();
        // 创建TableEnvironment
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance()
                .withConfiguration(config)
                .build());

        // 创建catalog
        String catalogSql = "CREATE CATALOG paimon_hive WITH (\n" +
                "    'type' = 'paimon',\n" +
                "    'metastore' = 'hive',\n" +
                "    'uri' = 'thrift://192.168.10.102:9083',\n" +
                "    'warehouse' = 'hdfs://192.168.10.102/user/hive/warehouse'\n" +
                ");";
        tableEnv.executeSql(catalogSql);
        System.out.println("catalog创建成功");

        // 使用指定的catalog和database
        tableEnv.useCatalog("paimon_hive");
        tableEnv.executeSql("create DATABASE IF NOT EXISTS dws;");
        tableEnv.useDatabase("dws");

        //创建piamon表
        String piamonSql = "CREATE TABLE IF NOT EXISTS dws.dws_traffic_page_visitor_page_view_nd_full(\n" +
                "    `mid_id`          string COMMENT '访客id',\n" +
                "    `k1`              string  COMMENT '分区字段',\n" +
                "    `brand`           string comment '手机品牌',\n" +
                "    `model`           string comment '手机型号',\n" +
                "    `operate_system`  string comment '操作系统',\n" +
                "    `page_id`         string COMMENT '页面id',\n" +
                "    `during_time_30d` BIGINT COMMENT '最近30日浏览时长',\n" +
                "    `view_count_30d`  BIGINT COMMENT '最近30日访问次数',\n" +
                "    PRIMARY KEY (`mid_id`,`k1` ) NOT ENFORCED\n" +
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
        tableEnv.executeSql(piamonSql);
        System.out.println("paimon表创建成功");

        //导入
        String insertSQL = "insert into dws.dws_traffic_page_visitor_page_view_nd_full select\n" +
                "    `mid_id`          ,--string COMMENT '访客id',\n" +
                "    `k1`              ,--string  COMMENT '分区字段',\n" +
                "    `brand`           ,--string comment '手机品牌',\n" +
                "    `model`           ,--string comment '手机型号',\n" +
                "    `operate_system`  ,--string comment '操作系统',\n" +
                "    `page_id`         ,--string COMMENT '页面id',\n" +
                "    sum(during_time)`during_time_30d` ,--BIGINT COMMENT '最近30日浏览时长',\n" +
                "    count(*)`view_count_30d`  --BIGINT COMMENT '最近30日访问次数',\n" +
                "from\n" +
                "(\n" +
                "    select\n" +
                "        mid_id,\n" +
                "        k1,\n" +
                "        brand,\n" +
                "        model,\n" +
                "        operate_system,\n" +
                "        page_id,\n" +
                "        during_time\n" +
                "    from dwd.dwd_traffic_action_full\n" +
                "    where timestampdiff(day,to_timestamp(k1,'yyyy-MM-dd'),to_timestamp('2024-05-01','yyyy-MM-dd')) between 0 and 30\n" +
                ") t\n" +
                "group by mid_id,k1,brand,model,operate_system,page_id";

        tableEnv.executeSql(insertSQL);
        System.out.println("dws_traffic_page_visitor_page_view_nd_full导入任务启动成功");

    }
}
