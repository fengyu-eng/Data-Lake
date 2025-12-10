package com.fy.warehouse.DIM;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.concurrent.ExecutionException;

public class dim_activity_full {
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
        System.out.println("创建catalog成功");
        tableEnv.useCatalog("paimon_hive");
        tableEnv.useDatabase("dim");


        String dropTableSql = "DROP TABLE IF EXISTS dim.dim_activity_full;";
        tableEnv.executeSql(dropTableSql);
        System.out.println("原有表删除成功");





        //创建paimon表
        String paimonDDL = "CREATE TABLE IF NOT EXISTS dim.dim_activity_full(\n" +
                "    `activity_rule_id`   INT COMMENT '活动规则ID',\n" +
                "    `activity_id`        BIGINT COMMENT '活动ID',\n" +
                "    `k1`                 STRING  COMMENT '分区字段',\n" +
                "    `activity_name`      STRING COMMENT '活动名称',\n" +
                "    `activity_type_code` STRING COMMENT '活动类型编码',\n" +
                "    `activity_type_name` STRING COMMENT '活动类型名称',\n" +
                "    `activity_desc`      STRING COMMENT '活动描述',\n" +
                "    `start_time`         STRING COMMENT '开始时间',\n" +
                "    `end_time`           STRING COMMENT '结束时间',\n" +
                "    `create_time`        STRING COMMENT '创建时间',\n" +
                "    `condition_amount`   DECIMAL(16, 2) COMMENT '满减金额',\n" +
                "    `condition_num`      BIGINT COMMENT '满减件数',\n" +
                "    `benefit_amount`     DECIMAL(16, 2) COMMENT '优惠金额',\n" +
                "    `benefit_discount`   DECIMAL(16, 2) COMMENT '优惠折扣',\n" +
                "    `benefit_rule`       STRING COMMENT '优惠规则',\n" +
                "    `benefit_level`      BIGINT COMMENT '优惠级别',\n" +
                "    PRIMARY KEY (`activity_rule_id`,`activity_id`,`k1` ) NOT ENFORCED\n" +
                "    )   PARTITIONED BY (`k1` ) WITH (\n" +
                "    'connector' = 'paimon',\n" +
                "    'metastore.partitioned-table' = 'true',\n" +
                "    'file.format' = 'parquet',\n" +
                "    'write-buffer-size' = '100mb',\n" +
                "    'write-buffer-spillable' = 'true' ,\n" +
                "    'write.async' = 'true', -- 优化4：开启异步写入（关键性能提升）\n" +
                "    'write.batch-size' = '100', -- 优化5：批量写入大小10000条\n" +
                "    'partition.expiration-time' = '1 d',\n" +
                "    'partition.expiration-check-interval' = '1 h',\n" +
                "    'partition.timestamp-formatter' = 'yyyy-MM-dd',\n" +
                "    'partition.timestamp-pattern' = '$k1'\n" +
                "    );";

        tableEnv.executeSql(paimonDDL);
        System.out.println("创建paimon表成功");


//        String verifySql = "select\n" +
//                "    rule.id,\n" +
//                "    info.id,\n" +
//                "    info.k1,\n" +
//                "    activity_name,\n" +
//                "    rule.activity_type,\n" +
//                "    dic.dic_name,\n" +
//                "    activity_desc,\n" +
//                "    start_time,\n" +
//                "    end_time,\n" +
//                "    create_time,\n" +
//                "    condition_amount,\n" +
//                "    condition_num,\n" +
//                "    benefit_amount,\n" +
//                "    benefit_discount,\n" +
//                "    case rule.activity_type\n" +
//                "        when '3101' then concat('满',cast(condition_amount as STRING),'元减',cast(benefit_amount as STRING),'元')\n" +
//                "        when '3102' then concat('满',cast(condition_num as STRING),'件打',cast((10*(1-benefit_discount)) as STRING),'折')\n" +
//                "        when '3103' then concat('打',cast((10*(1-benefit_discount)) as STRING),'折')\n" +
//                "        end benefit_rule,\n" +
//                "    benefit_level\n" +
//                "from\n" +
//                "    (\n" +
//                "        select\n" +
//                "            id,\n" +
//                "            activity_id,\n" +
//                "            activity_type,\n" +
//                "            condition_amount,\n" +
//                "            condition_num,\n" +
//                "            benefit_amount,\n" +
//                "            benefit_discount,\n" +
//                "            benefit_level\n" +
//                "        from ods.ods_activity_rule_full\n" +
//                "    )rule\n" +
//                "        left join\n" +
//                "    (\n" +
//                "        select\n" +
//                "            id,\n" +
//                "            k1,\n" +
//                "            activity_name,\n" +
//                "            activity_type,\n" +
//                "            activity_desc,\n" +
//                "            start_time,\n" +
//                "            end_time,\n" +
//                "            create_time\n" +
//                "        from ods.ods_activity_info_full\n" +
//                "    )info\n" +
//                "on rule.activity_id=info.id\n" +
//                "    left join\n" +
//                "    (\n" +
//                "    select\n" +
//                "    dic_code,\n" +
//                "    dic_name\n" +
//                "    from ods.ods_base_dic_full\n" +
//                "    where parent_code='31'\n" +
//                "    )dic\n" +
//                "    on rule.activity_type=dic.dic_code;";
//
//        tableEnv.executeSql(verifySql).print();

        //导入
        String insertSql = "insert into dim.dim_activity_full(\n" +
                "    activity_rule_id,\n" +
                "    activity_id,\n" +
                "    k1,\n" +
                "    activity_name,\n" +
                "    activity_type_code,\n" +
                "    activity_type_name,\n" +
                "    activity_desc,\n" +
                "    start_time,\n" +
                "    end_time,\n" +
                "    create_time,\n" +
                "    condition_amount,\n" +
                "    condition_num,\n" +
                "    benefit_amount,\n" +
                "    benefit_discount,\n" +
                "    benefit_rule,\n" +
                "    benefit_level\n" +
                "    )\n" +
                "select\n" +
                "    rule.id,\n" +
                "    info.id,\n" +
                "    info.k1,\n" +
                "    activity_name,\n" +
                "    rule.activity_type,\n" +
                "    dic.dic_name,\n" +
                "    activity_desc,\n" +
                "    start_time,\n" +
                "    end_time,\n" +
                "    create_time,\n" +
                "    condition_amount,\n" +
                "    condition_num,\n" +
                "    benefit_amount,\n" +
                "    benefit_discount,\n" +
                "    case rule.activity_type\n" +
                "        when '3101' then concat('满',cast(condition_amount as STRING),'元减',cast(benefit_amount as STRING),'元')\n" +
                "        when '3102' then concat('满',cast(condition_num as STRING),'件打',cast((10*(1-benefit_discount)) as STRING),'折')\n" +
                "        when '3103' then concat('打',cast((10*(1-benefit_discount)) as STRING),'折')\n" +
                "        end benefit_rule,\n" +
                "    benefit_level\n" +
                "from\n" +
                "    (\n" +
                "        select\n" +
                "            id,\n" +
                "            activity_id,\n" +
                "            activity_type,\n" +
                "            condition_amount,\n" +
                "            condition_num,\n" +
                "            benefit_amount,\n" +
                "            benefit_discount,\n" +
                "            benefit_level\n" +
                "        from ods.ods_activity_rule_full\n" +
                "    )rule\n" +
                "        left join\n" +
                "    (\n" +
                "        select\n" +
                "            id,\n" +
                "            k1,\n" +
                "            activity_name,\n" +
                "            activity_type,\n" +
                "            activity_desc,\n" +
                "            start_time,\n" +
                "            end_time,\n" +
                "            create_time\n" +
                "        from ods.ods_activity_info_full\n" +
                "    )info\n" +
                "on rule.activity_id=info.id\n" +
                "    left join\n" +
                "    (\n" +
                "    select\n" +
                "    dic_code,\n" +
                "    dic_name\n" +
                "    from ods.ods_base_dic_full\n" +
                "    where  parent_code='31'\n" +
                "    )dic\n" +
                "    on rule.activity_type=dic.dic_code;";

        TableResult tableResult = tableEnv.executeSql(insertSql);
        System.out.println("dim_activity_full任务启动成功");
        tableResult.await();

    }
}
