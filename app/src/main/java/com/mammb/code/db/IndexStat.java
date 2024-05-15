package com.mammb.code.db;

public class IndexStat {
    private String name;
    private FieldName fieldName;
    private Layout layout;
    private Schema tableSchema;
    private Statistics.Stat stat;
    private Transaction tx;

    public IndexStat(String name, FieldName fieldName, Schema tableSchema,
            Transaction tx, Layout layout, Statistics.Stat stat) {
        this.name = name;
        this.fieldName = fieldName;
        this.tableSchema = tableSchema;
        this.layout = layout;
        this.stat = stat;
        this.tx = tx;
    }

    public Index open() {
        return Index.hashIndex(tx, name, tableSchema, fieldName);
    }

    public int blocksAccessed() {
        return -1;
    }

    public int recordsOutput() {
        return -1;
    }

    public int distinctValues(FieldName fieldName) {
        return -1;
    }

}
