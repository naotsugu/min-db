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
        return Index.hashIndex(tx, name, layout);
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

    private Schema createIdxSchema(String name, FieldName fieldName, Schema tableSchema) {

        final TableName TABLE_NAME = new TableName("table_catalog");
        final FieldName BLOCK = new FieldName("block");
        final FieldName ID = new FieldName("id");
        final FieldName DATA_VAL = new FieldName("data_val");

        Schema schema = new Schema(TABLE_NAME);
        schema.addIntField(BLOCK);
        schema.addIntField(ID);
        if (tableSchema.type(fieldName) == java.sql.Types.INTEGER) {
            schema.addIntField(DATA_VAL);
        } else {
            schema.addStringField(DATA_VAL, tableSchema.length(fieldName));
        }
        return schema;
    }

}
