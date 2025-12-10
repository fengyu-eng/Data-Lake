package com.fy.warehouse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class Test {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        // 加载resources目录的配置文件
        Configuration hadoopConf = new Configuration();
        FileSystem fs = FileSystem.get(hadoopConf);

        // 测试访问Paimon表的HDFS目录
        Path paimonPath = new Path("hdfs://192.168.10.102:8020/user/hive/warehouse/ods.db/ods_activity_rule_full");
        System.out.println("=== 测试HDFS访问 ===");
        System.out.println("目录是否存在：" + fs.exists(paimonPath));
        System.out.println("目录权限：" + fs.getFileStatus(paimonPath).getPermission());

        // 列出目录下的文件（如果存在）
        if (fs.exists(paimonPath)) {
            FileStatus[] files = fs.listStatus(paimonPath);
            System.out.println("目录下文件数：" + (files != null ? files.length : 0));
            for (FileStatus file : files) {
                System.out.println("文件名：" + file.getPath().getName());
            }
        }
        fs.close();
    }
}
