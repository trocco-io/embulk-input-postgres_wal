package org.embulk.input.postgresql_wal;

import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.input.postgresql_wal.model.LsnHolder;
import org.embulk.spi.*;
import org.embulk.spi.type.Types;
import org.postgresql.replication.LogSequenceNumber;

public class PostgresqlWalInputPlugin
        implements InputPlugin {
    private LogSequenceNumber lsn;

    @Override
    public ConfigDiff transaction(ConfigSource config,
                                  InputPlugin.Control control) {
        PluginTask task = config.loadConfig(PluginTask.class);

        // initialize connection
        ConnectionManager.setProperties(task.getHost(), task.getPort(),
                task.getDatabase(), task.getUser(), task.getPassword(), task.getOptions());

        Schema schema = buildSchema(task);
        int taskCount = 1;  // number of run() method calls

        return resume(task.dump(), schema, taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
                             Schema schema, int taskCount,
                             InputPlugin.Control control) {
        control.run(taskSource, schema, taskCount);

        ConfigDiff configDiff = Exec.newConfigDiff();
        if (LsnHolder.getLsn() != null){
            configDiff.set("from_lsn", LsnHolder.getLsn().asString());
        }

        return configDiff;
    }

    @Override
    public void cleanup(TaskSource taskSource,
                        Schema schema, int taskCount,
                        List<TaskReport> successTaskReports) {
    }

    @Override
    public TaskReport run(TaskSource taskSource,
                          Schema schema, int taskIndex,
                          PageOutput output) {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        try {
            ConnectionManager.createReplicationConnection();
            try (PageBuilder pageBuilder = getPageBuilder(schema, output)) {
                PostgresqlWalDumper postgresqlWalDumper = new PostgresqlWalDumper(task, pageBuilder, schema, ConnectionManager.getReplicationConnection());
                postgresqlWalDumper.start();
                pageBuilder.finish();
            }
        } catch (Exception e) {
            // TODO: handle error
            System.out.println(e.getMessage());
            throw new RuntimeException(e);
        }
        return Exec.newTaskReport();
    }

    @Override
    public ConfigDiff guess(ConfigSource config) {
        return Exec.newConfigDiff();
    }

    @VisibleForTesting
    protected PageBuilder getPageBuilder(final Schema schema, final PageOutput output) {
        return new PageBuilder(Exec.getBufferAllocator(), schema, output);
    }

    private Schema buildSchema(PluginTask task) {
        int i = 0;

        // add meta data
        ImmutableList.Builder<Column> builder = ImmutableList.builder();
        for (ColumnConfig column : task.getColumns().getColumns()) {
            Column outputColumn = new Column(i++, column.getName(), column.getType());
            builder.add(outputColumn);
        }
        // add meta data schema
        if (task.getEnableMetadataDeleted()) {
            Column deleteFlagColumn = new Column(i++, PostgresqlWalUtil.getDeleteFlagName(task), Types.BOOLEAN);
            builder.add(deleteFlagColumn);
        }

        if (task.getEnableMetadataFetchedAt()) {
            Column fetchedAtColumn = new Column(i++, PostgresqlWalUtil.getFetchedAtName(task), Types.TIMESTAMP);
            builder.add(fetchedAtColumn);
        }

        if (task.getEnableMetadataSeq()) {
            Column seqColumn = new Column(i++, PostgresqlWalUtil.getSeqName(task), Types.LONG);
            builder.add(seqColumn);
        }

        return new Schema(builder.build());
    }

}
