package com.mammb.code.db;

public class Metadata {
    private static Catalog catalog = new Catalog();

    public Metadata() {
    }

    public void init(Transaction tx) {
        catalog.create(tx);
    }

}
