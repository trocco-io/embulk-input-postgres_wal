package org.embulk.input.postgres_wal;

import org.postgresql.PGProperty;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class ConnectionManager {

    private static String server;
    private static Integer port;
    private static String database;
    private static String user;
    private static String password;
    private static Connection sqlConnection;
    private static Connection repConnection;

    public static void setProperties(String server, Integer port, String database, String user, String password) {
        ConnectionManager.server = server;
        ConnectionManager.database = database;
        ConnectionManager.user = user;
        ConnectionManager.port = port;

        if (password == null) {
            password = "";
        }
        ConnectionManager.password = password;
    }

    public static void createReplicationConnection() throws SQLException {
        String url = "jdbc:postgresql://" + ConnectionManager.server + "/" + ConnectionManager.database;

        Properties props = new Properties();

        PGProperty.USER.set(props, ConnectionManager.user);
        PGProperty.PASSWORD.set(props, ConnectionManager.password);
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4");
        PGProperty.REPLICATION.set(props, "database");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");

        Connection conn = null;

        conn = DriverManager.getConnection(url, props);

        ConnectionManager.repConnection = conn;
    }

    public static Connection getReplicationConnection() {
        return ConnectionManager.repConnection;
    }

    public static void closeReplicationConnection() throws Exception {
        ConnectionManager.repConnection.close();
    }

    public static void createSQLConnection() throws SQLException {
        String url = "jdbc:postgresql://" + ConnectionManager.server + "/" + ConnectionManager.database;

        Properties props = new Properties();

        props.setProperty("user", ConnectionManager.user);
        props.setProperty("password", ConnectionManager.password);

        Connection conn = null;

        conn = DriverManager.getConnection(url, props);


        ConnectionManager.sqlConnection = conn;
    }

    public static Connection getSQLConnection() {
        return ConnectionManager.sqlConnection;
    }

    public static void closeSQLConnection() throws Exception {
        ConnectionManager.sqlConnection.close();
    }
}