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

    public String getCurrentWalLSN() throws SQLException {
        String sql = "SELECT pg_current_wal_lsn()"; // SQL for PG 10+
        if (getMajorVersion() <= 9) {
            sql = "SELECT pg_current_xlog_location()";
        }
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        while (rs.next()) {
            return rs.getString(1);
        }
        // never reach here
        throw new RuntimeException("Could not fetch LSN");
    }

    public Map<String, String> getColumns(String schemaName, String tableName) throws SQLException {
        String sql = "SELECT column_name, data_type  FROM information_schema.columns WHERE table_schema = ? AND table_name = ?";
        PreparedStatement pstmt = con.prepareStatement(sql);
        pstmt.setString(1, schemaName);
        pstmt.setString(2, tableName);
        ResultSet rs = pstmt.executeQuery();
        Map<String, String> schema = new HashMap<>();

        while (rs.next()) {
            schema.put(rs.getString("column_name"), rs.getString("data_type"));
        }
        return schema;
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
