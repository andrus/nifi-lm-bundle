package org.example.processors.lm.util;

import org.apache.nifi.dbcp.DBCPService;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

/**
 * A wrapper around {@link DBCPService} that provides a {@link DataSource} implementation for
 * third-party integrations, such as DFLib.
 */
public class DBCPServiceDataSource implements DataSource {

    private DBCPService delegate;

    public DBCPServiceDataSource(DBCPService delegate) {
        this.delegate = delegate;
    }

    @Override
    public Connection getConnection() {
        return delegate.getConnection();
    }

    @Override
    public Connection getConnection(String username, String password) {
        return delegate.getConnection();
    }

    @Override
    public int getLoginTimeout() {
        return -1;
    }

    @Override
    public void setLogWriter(PrintWriter out) {
        // noop
    }

    @Override
    public void setLoginTimeout(int seconds) {
        // noop
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }
}
