package com.fy.warehouse.DIM;

import com.fy.warehouse.config.FlinkConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.concurrent.ExecutionException;

public class dim_sku_full {
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
        System.out.println("catalog创建成功");

        tableEnv.useCatalog("paimon_hive");
        tableEnv.useDatabase("dim");

        //创建paimon表
        String paimonSql = "CREATE TABLE IF NOT EXISTS dim.dim_sku_full(\n" +
                "    `id`                   BIGINT COMMENT 'sku_id',\n" +
                "    `k1`                   STRING COMMENT '分区字段',\n" +
                "    `price`                DECIMAL(16, 2) COMMENT '商品价格',\n" +
                "    `sku_name`             STRING COMMENT '商品名称',\n" +
                "    `sku_desc`             STRING COMMENT '商品描述',\n" +
                "    `weight`               DECIMAL(16, 2) COMMENT '重量',\n" +
                "    `is_sale`              INT COMMENT '是否在售',\n" +
                "    `spu_id`               BIGINT COMMENT 'spu编号',\n" +
                "    `spu_name`             STRING COMMENT 'spu名称',\n" +
                "    `category3_id`         BIGINT COMMENT '三级分类id',\n" +
                "    `category3_name`       STRING COMMENT '三级分类名称',\n" +
                "    `category2_id`         BIGINT COMMENT '二级分类id',\n" +
                "    `category2_name`       STRING COMMENT '二级分类名称',\n" +
                "    `category1_id`         BIGINT COMMENT '一级分类id',\n" +
                "    `category1_name`       STRING COMMENT '一级分类名称',\n" +
                "    `tm_id`                BIGINT COMMENT '品牌id',\n" +
                "    `tm_name`              STRING COMMENT '品牌名称',\n" +
                "    `attr_ids`             STRING COMMENT '平台属性',\n" +
                "    `sale_attr_ids`        STRING COMMENT '销售属性',\n" +
                "    `create_time`           TIMESTAMP(3) COMMENT '创建时间',\n" +
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

        tableEnv.executeSql(paimonSql);
        System.out.println("paimon表创建成功");
        //导入
        String insertSQl = "INSERT INTO dim.dim_sku_full(\n" +
                "    id, k1, price, sku_name, sku_desc, weight, is_sale,\n" +
                "    spu_id, spu_name, category3_id, category3_name,\n" +
                "    category2_id, category2_name, category1_id,\n" +
                "    category1_name, tm_id, tm_name, attr_ids,\n" +
                "    sale_attr_ids, create_time\n" +
                ")\n" +
                "SELECT\n" +
                "    s.id,\n" +
                "    s.k1,\n" +
                "    s.price,\n" +
                "    s.sku_name,\n" +
                "    s.sku_desc,\n" +
                "    s.weight,\n" +
                "    s.is_sale,\n" +
                "    s.spu_id,\n" +
                "    sp.spu_name,\n" +
                "    s.category3_id,\n" +
                "    c3.name AS category3_name,\n" +
                "    c3.category2_id,\n" +
                "    c2.name AS category2_name,\n" +
                "    c2.category1_id,\n" +
                "    c1.name AS category1_name,\n" +
                "    s.tm_id,\n" +
                "    tm.tm_name,\n" +
                "    cast(a.attr_ids as STRING),\n" +
                "    cast(sa.sale_attr_ids as STRING),\n" +
                "    s.create_time\n" +
                "FROM\n" +
                "    (\n" +
                "        SELECT\n" +
                "            id,\n" +
                "            k1,\n" +
                "            price,\n" +
                "            sku_name,\n" +
                "            sku_desc,\n" +
                "            weight,\n" +
                "            is_sale,\n" +
                "            spu_id,\n" +
                "            category3_id,\n" +
                "            tm_id,\n" +
                "            create_time\n" +
                "        FROM ods.ods_sku_info_full\n" +
                "    ) s\n" +
                "        LEFT JOIN (\n" +
                "        SELECT\n" +
                "            id,\n" +
                "            spu_name\n" +
                "        FROM ods.ods_spu_info_full\n" +
                "    ) sp ON s.spu_id = sp.id\n" +
                "        LEFT JOIN (\n" +
                "        SELECT\n" +
                "            id,\n" +
                "            name,\n" +
                "            category2_id\n" +
                "        FROM ods.ods_base_category3_full\n" +
                "    ) c3 ON s.category3_id = c3.id\n" +
                "        LEFT JOIN (\n" +
                "        SELECT\n" +
                "            id,\n" +
                "            name,\n" +
                "            category1_id\n" +
                "        FROM ods.ods_base_category2_full\n" +
                "    ) c2 ON c3.category2_id = c2.id\n" +
                "        LEFT JOIN (\n" +
                "        SELECT\n" +
                "            id,\n" +
                "            name\n" +
                "        FROM ods.ods_base_category1_full\n" +
                "    ) c1 ON c2.category1_id = c1.id\n" +
                "        LEFT JOIN (\n" +
                "        SELECT\n" +
                "            id,\n" +
                "            tm_name\n" +
                "        FROM ods.ods_base_trademark_full\n" +
                "    ) tm ON s.tm_id = tm.id\n" +
                "        LEFT JOIN (\n" +
                "        SELECT\n" +
                "            sku_id,\n" +
                "            collect(id) AS attr_ids\n" +
                "        FROM ods.ods_sku_attr_value_full\n" +
                "        GROUP BY sku_id\n" +
                "    ) a ON s.id = a.sku_id\n" +
                "        LEFT JOIN (\n" +
                "        SELECT\n" +
                "            sku_id,\n" +
                "            collect(id) AS sale_attr_ids\n" +
                "        FROM ods.ods_sku_sale_attr_value_full\n" +
                "        GROUP BY sku_id\n" +
                "    ) sa ON s.id = sa.sku_id;";
        TableResult tableResult = tableEnv.executeSql(insertSQl);
        System.out.println("dim_sku_full表导入任务启动成功");
        tableResult.await();

    }
}
