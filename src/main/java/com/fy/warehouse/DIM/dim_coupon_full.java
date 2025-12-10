package com.fy.warehouse.DIM;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.concurrent.ExecutionException;

public class dim_coupon_full {
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
        System.out.println("paimon-hive catalog创建成功");

        tableEnv.useCatalog("paimon_hive");
        tableEnv.useDatabase("dim");

        //创建paimon表
        String paimonSQL = "CREATE TABLE IF NOT EXISTS dim.dim_coupon_full(\n" +
                "    `id`               BIGINT COMMENT '购物券编号',\n" +
                "    `k1`               STRING COMMENT '分区字段',\n" +
                "    `coupon_name`      STRING COMMENT '购物券名称',\n" +
                "    `coupon_type_code` STRING COMMENT '购物券类型编码',\n" +
                "    `coupon_type_name` STRING COMMENT '购物券类型名称',\n" +
                "    `condition_amount` DECIMAL(16, 2) COMMENT '满额数',\n" +
                "    `condition_num`    BIGINT COMMENT '满件数',\n" +
                "    `activity_id`      BIGINT COMMENT '活动编号',\n" +
                "    `benefit_amount`   DECIMAL(16, 2) COMMENT '减金额',\n" +
                "    `benefit_discount` DECIMAL(16, 2) COMMENT '折扣',\n" +
                "    `benefit_rule`     STRING COMMENT '优惠规则:满元*减*元，满*件打*折',\n" +
                "    `create_time`      TIMESTAMP(3) COMMENT '创建时间',\n" +
                "    `range_type_code`  STRING COMMENT '优惠范围类型编码',\n" +
                "    `range_type_name`  STRING COMMENT '优惠范围类型名称',\n" +
                "    `limit_num`        BIGINT COMMENT '最多领取次数',\n" +
                "    `taken_count`      BIGINT COMMENT '已领取次数',\n" +
                "    `start_time`       TIMESTAMP(3) COMMENT '可以领取的开始日期',\n" +
                "    `end_time`         TIMESTAMP(3) COMMENT '可以领取的结束日期',\n" +
                "    `operate_time`     TIMESTAMP(3) COMMENT '修改时间',\n" +
                "    `expire_time`      TIMESTAMP(3) COMMENT '过期时间',\n" +
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

        //导入
        String insertSQL = "insert into dim.dim_coupon_full(id, k1, coupon_name, coupon_type_code, coupon_type_name, condition_amount, condition_num, activity_id, benefit_amount, benefit_discount, benefit_rule, create_time, range_type_code, range_type_name, limit_num, taken_count, start_time, end_time, operate_time, expire_time)\n" +
                "select\n" +
                "    id,\n" +
                "    k1,\n" +
                "    coupon_name,\n" +
                "    coupon_type,\n" +
                "    coupon_dic.dic_name,\n" +
                "    condition_amount,\n" +
                "    condition_num,\n" +
                "    activity_id,\n" +
                "    benefit_amount,\n" +
                "    benefit_discount,\n" +
                "    case coupon_type\n" +
                "        when '3201' then concat('满',cast(condition_amount as STRING),'元减', cast(benefit_amount as STRING),'元')\n" +
                "        when '3202' then concat('满',cast(condition_num as STRING),'件打',cast((10*(1-benefit_discount)) as STRING),'折')\n" +
                "        when '3203' then concat('减',cast(benefit_amount as STRING),'元')\n" +
                "        end benefit_rule,\n" +
                "    create_time,\n" +
                "    range_type,\n" +
                "    range_dic.dic_name,\n" +
                "    limit_num,\n" +
                "    taken_count,\n" +
                "    start_time,\n" +
                "    end_time,\n" +
                "    operate_time,\n" +
                "    expire_time\n" +
                "from\n" +
                "    (\n" +
                "        select\n" +
                "            id,\n" +
                "            k1,\n" +
                "            coupon_name,\n" +
                "            coupon_type,\n" +
                "            condition_amount,\n" +
                "            condition_num,\n" +
                "            activity_id,\n" +
                "            benefit_amount,\n" +
                "            benefit_discount,\n" +
                "            create_time,\n" +
                "            range_type,\n" +
                "            limit_num,\n" +
                "            taken_count,\n" +
                "            start_time,\n" +
                "            end_time,\n" +
                "            operate_time,\n" +
                "            expire_time\n" +
                "        from ods.ods_coupon_info_full\n" +
                "    )ci\n" +
                "        left join\n" +
                "    (\n" +
                "        select\n" +
                "            dic_code,\n" +
                "            dic_name\n" +
                "        from ods.ods_base_dic_full\n" +
                "        where parent_code='32'\n" +
                "    )coupon_dic\n" +
                "on ci.coupon_type=coupon_dic.dic_code\n" +
                "    left join\n" +
                "    (\n" +
                "    select\n" +
                "    dic_code,\n" +
                "    dic_name\n" +
                "    from ods.ods_base_dic_full\n" +
                "    where parent_code='33'\n" +
                "    )range_dic\n" +
                "    on ci.range_type=range_dic.dic_code;";

        TableResult tableResult = tableEnv.executeSql(insertSQL);
        System.out.println("dim_coupon_full导入任务启动成功");
        tableResult.await();
    }
}
