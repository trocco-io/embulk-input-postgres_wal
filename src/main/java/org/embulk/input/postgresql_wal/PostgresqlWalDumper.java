package org.embulk.input.postgresql_wal;

import com.google.common.annotations.VisibleForTesting;
import org.embulk.input.postgresql_wal.decoders.Wal2JsonDecoderPlugin;
import org.embulk.input.postgresql_wal.model.*;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class PostgresqlWalDumper {
    private PluginTask task;
    private PageBuilder pageBuilder;
    private Schema schema;
    private PostgresqlWalClient walClient;
    private Connection connection;
    private Wal2JsonDecoderPlugin decoderPlugin;
    private static final Logger logger = LoggerFactory.getLogger(PostgresqlWalDumper.class);
    PGReplicationStream stream;

    public PostgresqlWalDumper(PluginTask task, PageBuilder pageBuilder, Schema schema, Connection connection) {
        this.task = task;
        this.pageBuilder = pageBuilder;
        this.schema = schema;
        this.connection = connection;
        this.decoderPlugin = new Wal2JsonDecoderPlugin();
    }

    public void start() {
        try {
            walClient = new PostgresqlWalClient(connection);
            stream = walClient.getReplicationStream(task.getSlot());
            long waitMin = task.getWalInitialWait();
            int retryCount = 0;
            long wait = waitMin;
            while (!stream.isClosed()) {
                ByteBuffer msg = stream.readPending(); // non-blocking
                // stop if exceed LSN or time out
                if (msg == null) {
                    TimeUnit.MILLISECONDS.sleep(wait);
                    retryCount += 1;
                    wait = waitMin * (long)Math.pow(2, retryCount);
                    if (wait > task.getWalReadTimeout()){
                        logger.info("WAL time out exceeded. Stop retrieving WAL");
                        break;
                    }
                    continue;
                }else{
                    retryCount = 0;
                    wait = waitMin;
                }

                List<AbstractRowEvent> rowEvents = decoderPlugin.decode(msg, stream.getLastReceiveLSN());
                for (AbstractRowEvent rowEvent: rowEvents) {
                    handleRowEvent(rowEvent);
                    LsnHolder.setLsn(rowEvent.getNextLogSequenceNumber());
                }
                pageBuilder.flush();

                if (task.getToLsn().isPresent()){
                    if (LsnHolder.getLsn().asLong() >= LogSequenceNumber.valueOf(task.getToLsn().get()).asLong()){
                        logger.info("LSN exceeded to_lsn: {}, current_lsn: {}",
                                task.getToLsn().get(),
                                LsnHolder.getLsn().asString());
                        break;
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }finally {
            try {
                if (stream != null && !stream.isClosed()){
                    stream.close();
                }
                if (ConnectionManager.getReplicationConnection() != null && !ConnectionManager.getReplicationConnection().isClosed()){
                    ConnectionManager.getReplicationConnection().close();
                }
            }catch (SQLException se){
                se.printStackTrace();
            }
        }
    }

    @VisibleForTesting
    public void addRow(List<Column> columns, boolean deleteFlag) {
        if (task.getEnableMetadataDeleted()) {
            Column deleteFlagColumn = new Column(PostgresqlWalUtil.getDeleteFlagName(task), String.valueOf(deleteFlag), "boolean");
            columns.add(deleteFlagColumn);
        }

        if (task.getEnableMetadataSeq()) {
            Column seqColumn = new Column(PostgresqlWalUtil.getSeqName(task), String.valueOf(PostgresqlWalUtil.getSeqCounter().incrementAndGet()), "long");
            columns.add(seqColumn);
        }

        schema.visitColumns(new PostgresqlWalColumnVisitor(new PostgresqlWalAccessor(columns), pageBuilder, task));
        pageBuilder.addRecord();
    }

    @VisibleForTesting
    public void handleRowEvent(AbstractRowEvent rowEvent) {
        if (rowEvent.getEventType() == null) {
            return;
        }
        if (!tableFilter(rowEvent)){
            return;
        }
        switch (rowEvent.getEventType()) {
            case INSERT:
                handleInsert((InsertRowEvent) rowEvent);
                break;
            case UPDATE:
                handleUpdate((UpdateRowEvent) rowEvent);
                break;
            case DELETE:
                handleDelete((DeleteRowEvent) rowEvent);
                break;
            default:
                throw new RuntimeException("never reach here");

        }
    }

    public boolean tableFilter(AbstractRowEvent rowEvent){
        return (task.getSchema().equals(rowEvent.getSchemaName()) && task.getTable().equals(rowEvent.getTableName()));
    }

    @VisibleForTesting
    public void handleInsert(InsertRowEvent insertRowEvent) {
        addRow(insertRowEvent.getColumns(), false);
    }

    @VisibleForTesting
    public void handleUpdate(UpdateRowEvent updateRowEvent) {
        addRow(updateRowEvent.getPrimaryKeyColumns(), true);
        addRow(updateRowEvent.getColumns(), false);

    }

    @VisibleForTesting
    public void handleDelete(DeleteRowEvent deleteRowEvent) {
        addRow(deleteRowEvent.getPrimaryKeyColumns(), true);
    }
}
