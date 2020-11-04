package org.embulk.input.postgresql_wal;

import com.google.common.annotations.VisibleForTesting;
import org.embulk.input.postgresql_wal.decoders.Wal2JsonDecoderPlugin;
import org.embulk.input.postgresql_wal.model.AbstractRowEvent;
import org.embulk.input.postgresql_wal.model.DeleteRowEvent;
import org.embulk.input.postgresql_wal.model.InsertRowEvent;
import org.embulk.input.postgresql_wal.model.UpdateRowEvent;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PostgresqlWalDumper {
    private PluginTask task;
    private PageBuilder pageBuilder;
    private Schema schema;
    private ConnectionManager connectionManager;
    private PostgresqlWalClient walClient;
    private Connection connection;
    private Wal2JsonDecoderPlugin decoderPlugin;

    public PostgresqlWalDumper(PluginTask task, PageBuilder pageBuilder, Schema schema, Connection connection){
        this.task = task;
        this.pageBuilder = pageBuilder;
        this.schema = schema;
        this.connection = connection;
        this.decoderPlugin = new Wal2JsonDecoderPlugin();
    }

    public void start(){
        try{
            walClient = new PostgresqlWalClient(connection);
            // System.out.println(client.getCurrentWalLSN());
            // System.out.println(client.getMajorVersion());

            PGReplicationStream stream = walClient.getReplicationStream(task.getSlot());
            while(!stream.isClosed()){
                ByteBuffer msg = stream.readPending(); // non-blocking
                if (msg == null) {
                    TimeUnit.MILLISECONDS.sleep(10L);
                    continue;
                }
                AbstractRowEvent rowEvent = decoderPlugin.decode(msg, stream.getLastReceiveLSN());
                handleRowEvent(rowEvent);

                // should be update?
                // stream.setAppliedLSN(stream.getLastReceiveLSN());
                // stream.setFlushedLSN(stream.getLastReceiveLSN());
                LogSequenceNumber lsn = stream.getLastAppliedLSN();
                System.out.println(lsn);
                System.out.println(lsn.asString());
                System.out.println(lsn.asLong());
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    @VisibleForTesting
    public void addRows(Map<String, String> row, boolean deleteFlag){
        // TODO: add meta data
        if(task.getEnableMetadataDeleted()){
            row.put(PostgresqlWalUtil.getDeleteFlagName(task), String.valueOf(deleteFlag));
        }

        if (task.getEnableMetadataSeq()){
            row.put(PostgresqlWalUtil.getSeqName(task), String.valueOf(PostgresqlWalUtil.getSeqCounter().incrementAndGet()));
        }

        schema.visitColumns(new PostgresqlWalColumnVisitor(new PostgresqlWalAccessor(row), pageBuilder, task));
        pageBuilder.addRecord();
    }

    @VisibleForTesting
    public void handleRowEvent(AbstractRowEvent rowEvent) {
        switch (rowEvent.getEventType()){
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

    @VisibleForTesting
    public void handleInsert(InsertRowEvent insertRowEvent){
        addRows(insertRowEvent.getFields(), false);
    }

    @VisibleForTesting
    public void handleUpdate(UpdateRowEvent updateRowEvent){
        addRows(updateRowEvent.getPrimaryKeys(), true);
        addRows(updateRowEvent.getFields(), false);

    }

    @VisibleForTesting
    public void handleDelete(DeleteRowEvent deleteRowEvent){
        addRows(deleteRowEvent.getPrimaryKeys(), true);
    }
}
