package com.fy.warehouse.config;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.StateBackendOptions;

//Flink配置类
public class FlinkConfigUtil {
    public static Configuration getFlinkConfig(){
        Configuration config = new Configuration();
        System.setProperty("HADOOP_USER_NAME", "root"); // 设置 Hadoop 访问用户

        // 关键配置1：指定Hive版本（必须与集群Hive版本一致）
        config.setString("hive.version", "3.1.3");

        // 关键配置2：启用Hive兼容模式
        config.setString("table.exec.hive.fallback-mapred-reader", "true");
        // 关键配置3：关闭分区剪枝（避免Flink扫描分区时卡住）
        config.setString("table.optimizer.partition-pruning", "false");
        // 基础运行配置
        config.setString("execution.runtime-mode", "STREAMING");
        config.setString("table.exec.sink.upsert-materialize", "NONE");
        config.setInteger("parallelism.default", 1); // 并行度
        // ========== 关键修改1：开启Checkpoint（原配置为false，导致状态无法持久化） ==========
        config.setBoolean("execution.checkpointing.enabled", true);
        // ========== 关键修改2：配置Checkpoint存储为文件系统（解决状态超限） ==========
        // 设置状态后端为文件系统（或RocksDB，推荐大状态场景）
        config.setString(StateBackendOptions.STATE_BACKEND, "filesystem");
        // Checkpoint存储路径（HDFS路径，需确保root用户有读写权限）
        config.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "hdfs://192.168.10.102:8020/flink/checkpoints");
        // 可选：如果使用RocksDB状态后端（更适合大状态，注释掉上面的filesystem，启用下面两行）
        // config.setString(StateBackendOptions.STATE_BACKEND, "rocksdb");
        // config.setString(StateBackendOptions.ROCKSDB_CHECKPOINT_DIR, "hdfs://192.168.10.102:8020/flink/rocksdb-checkpoints");

        // ========== Checkpoint优化配置 ==========
        config.setString("taskmanager.memory.process.size", "1024m");
        config.setString("taskmanager.memory.task.heap.size", "512m");
        config.setString("execution.checkpointing.interval", "60s"); // 检查点间隔
        config.setString("execution.checkpointing.timeout", "300s"); // Checkpoint超时时间（5分钟，避免超时）
        config.setInteger("execution.checkpointing.tolerable-failure-number", 3); // 容忍3次Checkpoint失败
        config.setString("execution.checkpointing.mode", "EXACTLY_ONCE"); // 精确一次语义（默认）

        // ========== 状态TTL配置 ==========
        config.setString("table.exec.state.ttl", "86400000"); // 状态 TTL（24小时）

        // ========== 微批配置 ==========
        config.setString("table.exec.mini-batch.enabled", "true"); // 启用微批
        config.setString("table.exec.mini-batch.allow-latency", "500ms"); // 微批延迟
        config.setString("table.exec.mini-batch.size", "1000"); // 微批大小

        // ========== 时区&数据约束 ==========
        config.setString("table.local-time-zone", "Asia/Shanghai"); // 时区
        config.setString("table.exec.sink.not-null-enforcer", "DROP"); // 非空约束

        // ========== Paimon配置 ==========
        config.setString("paimon.sink.batch-size", "100"); // 小批次快速写入
        config.setString("paimon.sink.buffer-time", "0s"); // 关闭缓冲等待
        config.setString("paimon.file.size", "64mb"); // 小文件即可，避免合并

        // ========== 内存&执行配置 ==========
        config.setString("exec_mem_limit", "536870912");
        config.setInteger("parallel_fragment_exec_instance_num", 1);
        config.setInteger("query_timeout", 60); // 超时时间=60秒

        // ========== 关键修改3：HDFS RPC超时配置（解决RPC中断） ==========
        config.setString("env.java.opts", "-Ddfs.client.socket-timeout=300000 -Dipc.client.connect.timeout=300000 -Dipc.client.connect.max.retries=5");

        // 关键：Hadoop 核心配置（必须在创建 TableEnvironment 前设置）
        config.setString("fs.defaultFS", "hdfs://192.168.10.102:8020"); // HDFS 地址
        config.setString("hadoop.user.name", "root"); // 再次明确 Hadoop 用户（避免冲突）

        // Hive Metastore 配置
        config.setString("hive.metastore.uris", "thrift://192.168.10.102:9083"); // Hive Metastore 地址
        return config;
    }
}