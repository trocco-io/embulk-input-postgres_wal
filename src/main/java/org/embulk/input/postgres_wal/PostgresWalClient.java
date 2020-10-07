package org.embulk.input.postgres_wal;

import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;

import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class PostgresWalClient {
    private static Properties props;
    private Connection con;

    public PostgresWalClient(Connection con) throws SQLException {
        this.con = con;
    }

    public int getMajorVersion() throws SQLException {
        DatabaseMetaData dbmd = con.getMetaData();
        return dbmd.getDatabaseMajorVersion();
    }

    public String getCurrentWalLSN() throws SQLException{
        String sql = "SELECT pg_current_wal_lsn()"; // SQL for PG 10+
        if (getMajorVersion() <= 9){
            sql = "SELECT pg_current_xlog_location()";
        }
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        while(rs.next()){
            return rs.getString(1);
        }
        // never reach here
        throw new RuntimeException("Could not fetch LSN");
    }

    public Map<String, String> getColumns(String schemaName, String tableName) throws SQLException{
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

    public PGReplicationStream getReplicationStream(String slotName) throws  SQLException{
        PGConnection pgConnection = con.unwrap(PGConnection.class);
        return pgConnection.getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName(slotName)
                // .withStartPosition(LogSequenceNumber.valueOf(lsn))
                .withSlotOption("include-lsn", true)
                .withSlotOption("include-pk", true)
                .withStatusInterval(20, TimeUnit.SECONDS)
                .start();
    }


}
