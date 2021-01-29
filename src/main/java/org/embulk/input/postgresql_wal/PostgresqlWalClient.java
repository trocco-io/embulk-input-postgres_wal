package org.embulk.input.postgresql_wal;

import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;

import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class PostgresqlWalClient {
    private Connection con;

    public PostgresqlWalClient(Connection con) throws SQLException {
        this.con = con;
    }

    public int getMajorVersion() throws SQLException {
        DatabaseMetaData dbmd = con.getMetaData();
        return dbmd.getDatabaseMajorVersion();
    }

    public PGReplicationStream getReplicationStream(String slotName, String fromLsn) throws SQLException {
        PGConnection pgConnection = con.unwrap(PGConnection.class);
        ChainedLogicalStreamBuilder builder = pgConnection.getReplicationAPI().replicationStream().logical().withSlotName(slotName)
                .withSlotOption("include-lsn", true)
                .withStatusInterval(20, TimeUnit.SECONDS);
        if (fromLsn != null){
            builder.withStartPosition(LogSequenceNumber.valueOf(fromLsn));
        }
        return builder.start();
    }

    public PGReplicationStream getReplicationStream(String slotName) throws SQLException {
        return getReplicationStream(slotName, null);
    }
}
