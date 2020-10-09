package org.embulk.input.postgres_wal;

import java.util.List;

import com.google.common.base.Optional;

import com.google.common.collect.ImmutableList;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.*;
import org.embulk.spi.type.Types;

public class PostgresWalInputPlugin
        implements InputPlugin
{

    @Override
    public ConfigDiff transaction(ConfigSource config,
            InputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        // initialize connection
        ConnectionManager.setProperties(task.getHost(), task.getPort(),
                task.getDatabase(), task.getUser(), task.getPassword(), task.getOptions());

        Schema schema = task.getColumns().toSchema();
        int taskCount = 1;  // number of run() method calls

        return resume(task.dump(), schema, taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            InputPlugin.Control control)
    {
        control.run(taskSource, schema, taskCount);
        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource,
            Schema schema, int taskCount,
            List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TaskReport run(TaskSource taskSource,
            Schema schema, int taskIndex,
            PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);


        // Write your code here :)
        throw new UnsupportedOperationException("PostgresWalInputPlugin.run method is not implemented yet");
    }

    @Override
    public ConfigDiff guess(ConfigSource config)
    {
        return Exec.newConfigDiff();
    }


    private Schema buildSchema(PluginTask task){
        int i = 0;

        // add meta data
        ImmutableList.Builder<Column> builder = ImmutableList.builder();
        for (ColumnConfig column : task.getColumns().getColumns()) {
            Column outputColumn = new Column(i++, column.getName(), column.getType());
            builder.add(outputColumn);
        }
        // add meta data schema
        if (task.getEnableMetadataDeleted()){
            Column deleteFlagColumn = new Column(i++, PostgresWalUtil.getDeleteFlagName(task), Types.BOOLEAN);
            builder.add(deleteFlagColumn);
        }

        if (task.getEnableMetadataFetchedAt()){
            Column fetchedAtColumn = new Column(i++, PostgresWalUtil.getFetchedAtName(task), Types.TIMESTAMP);
            builder.add(fetchedAtColumn);
        }

        if (task.getEnableMetadataSeq()){
            Column seqColumn = new Column(i++, PostgresWalUtil.getSeqName(task), Types.LONG);
            builder.add(seqColumn);
        }

        return new Schema(builder.build());
    }

}
