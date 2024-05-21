package com.mammb.code.db.jdbc;

import com.mammb.code.db.Schema;
import java.sql.*;

public class EmbeddedMetaData implements ResultSetMetaData {
    private final Schema schema;

    public EmbeddedMetaData(Schema schema) {
        this.schema = schema;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return schema.fields().size();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return schema.get(column - 1).val();
    }
    @Override
    public int getColumnType(int column) throws SQLException {
        return schema.type(schema.get(column - 1));
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        var fieldName = schema.get(column - 1);
        int len = schema.type(fieldName) == Types.INTEGER
            ? 6
            : schema.length(fieldName);
        return Math.max(fieldName.val().length(), len) + 1;
    }


    // ------------------------------------------------------------------------

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return 0;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return "";
    }


    @Override
    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return "";
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return "";
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return "";
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return "";
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
