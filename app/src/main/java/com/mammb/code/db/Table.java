package com.mammb.code.db;

public class Table {
    private Transaction tx;
    private Layout layout;

    public Table(Transaction tx, Layout layout) {
        this.tx = tx;
        this.layout = layout;
    }

    public void insert() {
    }
    public void close() {
    }

    public void setInt(FieldName name, int val) {
    }

    public void setString(FieldName name, String val) {
    }
}
