package com.mammb.code.db;

public class Metadata {
    private static Catalog catalog = new Catalog();
    private static Statistics statistics = new Statistics();

    public Metadata() {
    }

    public void init(Transaction tx) {
        catalog.create(tx);
    }

    public void createTable(TableName name, Schema schema, Transaction tx) {
        catalog.createTable(name, schema, tx);
    }

    public Layout getLayout(TableName name, Transaction tx) {
        return catalog.getLayout(name, tx);
    }

}
