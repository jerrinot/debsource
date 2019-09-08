package info.jerrinot.jet.cdc.support;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public final class JdbcTemplate implements AutoCloseable {
    private static String DRIVER = "com.mysql.jdbc.Driver";
    private final Connection connection;

    static {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            throw new AssertionError(e);
        }
    }

    public JdbcTemplate(int port, String username, String password) {
        try {
            connection = DriverManager.getConnection("jdbc:mysql://localhost:" + port, username, password);
        } catch (SQLException e) {
            throw new AssertionError(e);
        }
    }

    public void switchToDatabase(String name) {
        try {
            connection.setCatalog(name);
        } catch (SQLException e) {
            throw new AssertionError(e);
        }
    }

    public int executeUpdate(String update, Object...params) {
        PreparedStatement stmt = null;
        try {
            stmt = connection.prepareStatement(update);
            for (int i = 0; i < params.length; i++) {
                stmt.setObject(i + 1, params[i]);
            }
            return stmt.executeUpdate();
        } catch (SQLException e) {
            throw new AssertionError(e);
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public int newDatabase(String name) {
        return executeUpdate("create database " + name);
    }

    public int dropDatabase(String name) {
        return executeUpdate("drop database " + name);
    }

    @Override
    public void close() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }
}
