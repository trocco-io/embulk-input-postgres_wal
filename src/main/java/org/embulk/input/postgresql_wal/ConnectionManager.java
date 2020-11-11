package org.embulk.input.postgresql_wal;

import org.postgresql.PGProperty;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

public class ConnectionManager {

    private static String server;
    private static Integer port;
    private static String database;
    private static String user;
    private static String password;
    private static Connection sqlConnection;
    private static Connection repConnection;
    private static Map<String, String> options;

    public static void setProperties(String server, Integer port, String database, String user, String password, Map<String, String> options) {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (Exception ignored) {

        }
        ;
        ConnectionManager.server = server;
        ConnectionManager.database = database;
        ConnectionManager.user = user;
        ConnectionManager.port = port;

        if (password == null) {
            password = "";
        }
        ConnectionManager.password = password;
        ConnectionManager.options = options;
    }

    public static void createReplicationConnection() throws SQLException {
        String url = "jdbc:postgresql://" + ConnectionManager.server + ":" + ConnectionManager.port + "/" + ConnectionManager.database;

        Properties props = new Properties();
        PGProperty.USER.set(props, ConnectionManager.user);
        PGProperty.PASSWORD.set(props, ConnectionManager.password);
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4");
        PGProperty.REPLICATION.set(props, "database");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");
        props.putAll(ConnectionManager.options);

        ConnectionManager.repConnection = DriverManager.getConnection(url, props);
    }

    public static Connection getReplicationConnection() {
        return ConnectionManager.repConnection;
    }

    // comment out for future
    // public static void createSQLConnection() throws SQLException {
    //     String url = "jdbc:postgresql://" + ConnectionManager.server + ":" + ConnectionManager.port + "/" + ConnectionManager.database;

    //     Properties props = new Properties();

    //     props.setProperty("user", ConnectionManager.user);
    //     props.setProperty("password", ConnectionManager.password);
    //     props.putAll(ConnectionManager.options);

    //     ConnectionManager.sqlConnection = DriverManager.getConnection(url, props);
    // }

    // public static Connection getSQLConnection() {
    //     return ConnectionManager.sqlConnection;
    // }
}