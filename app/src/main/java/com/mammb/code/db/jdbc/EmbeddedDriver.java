package com.mammb.code.db.jdbc;

import com.mammb.code.db.DataBase;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class EmbeddedDriver implements Driver {

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        Path baseDirectory = Path.of(url.replace("jdbc:simpledb:", ""));
        DataBase db = new DataBase(baseDirectory);
        return new EmbeddedConnection(db);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        throw new SQLException("Operation not implemented");
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Operation not implemented");
    }
}
