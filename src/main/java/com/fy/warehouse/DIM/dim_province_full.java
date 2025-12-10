package com.fy.warehouse.DIM;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.concurrent.ExecutionException;

public class dim_province_full {
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
        String paimonSQL = "CREATE TABLE IF NOT EXISTS dim.dim_province_full(\n" +
                "    `id`            BIGINT COMMENT 'id',\n" +
                "    `province_name` STRING COMMENT '省市名称',\n" +
                "    `area_code`     STRING COMMENT '地区编码',\n" +
                "    `iso_code`      STRING COMMENT '旧版ISO-3166-2编码，供可视化使用',\n" +
                "    `iso_3166_2`    STRING COMMENT '新版IOS-3166-2编码，供可视化使用',\n" +
                "    `region_id`     STRING COMMENT '地区id',\n" +
                "    `region_name`   STRING COMMENT '地区名称',\n" +
                "    PRIMARY KEY (`id` ) NOT ENFORCED\n" +
                "    );";

        tableEnv.executeSql(paimonSQL);
        System.out.println("paimon表创建成功");

        //导入
        String insertSql = "insert into dim.dim_province_full(id, province_name, area_code, iso_code, iso_3166_2, region_id, region_name)\n" +
                "select\n" +
                "    province.id,\n" +
                "    province.name,\n" +
                "    province.area_code,\n" +
                "    province.iso_code,\n" +
                "    province.iso_3166_2,\n" +
                "    region_id,\n" +
                "    region_name\n" +
                "from\n" +
                "    (\n" +
                "        select\n" +
                "            id,\n" +
                "            name,\n" +
                "            region_id,\n" +
                "            area_code,\n" +
                "            iso_code,\n" +
                "            iso_3166_2\n" +
                "        from ods.ods_base_province_full\n" +
                "    )province\n" +
                "        left join\n" +
                "    (\n" +
                "        select\n" +
                "            id,\n" +
                "            region_name\n" +
                "        from ods.ods_base_region_full\n" +
                "    )region\n" +
                "    on province.region_id=region.id;";
        TableResult tableResult = tableEnv.executeSql(insertSql);
        System.out.println("dim_province_full表导入任务启动成功");
        tableResult.await();
    }
}
