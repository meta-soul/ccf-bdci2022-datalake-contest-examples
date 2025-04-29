package org.ccf.bdci2022.datalake_contest;

import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.table.api.*;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.util.concurrent.ExecutionException;

import static org.apache.flink.configuration.CoreOptions.DEFAULT_PARALLELISM;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM;

class FlinkBatchRead {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        org.apache.flink.configuration.Configuration config = new org.apache.flink.configuration.Configuration();
        config.set(DEFAULT_PARALLELISM, 4);
        config.set(TaskManagerOptions.NUM_TASK_SLOTS, 4);
        config.setString("s3.endpoint", "http://localhost:9000");
        config.setString("s3.access-key", "minioadmin1");
        config.setString("s3.secret-key", "minioadmin1");
        config.setString("s3.path.style.access", "true");
        TableEnvironment tableEnv = TableEnvironment.create(
                    EnvironmentSettings.newInstance().withConfiguration(config)
                            .inBatchMode().build());
        FileSystem.initialize(config, null);
        tableEnv.getConfig()
                .getConfiguration()
                .setInteger(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM.key(), 4);
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        LakeSoulCatalog catalog = new LakeSoulCatalog();
        tableEnv.registerCatalog("lakesoul", catalog);
        tableEnv.useCatalog("lakesoul");
        tableEnv.executeSql("create table `default_catalog`.`default_database`.`lakesoul_test_table` (" +
                ")" +
                "with ('connector'='blackhole') ");
        long startTime = System.currentTimeMillis();
        tableEnv.executeSql("insert into " +
                "`default_catalog`.`default_database`.`lakesoul_test_table` " +
                "select * from `default`.`lakesoul_test_table`").await();
        long endTime = System.currentTimeMillis();
        System.out.println("Execution time: " + (endTime - startTime));
    }
}