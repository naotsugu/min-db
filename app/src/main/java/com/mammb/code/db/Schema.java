package com.mammb.code.db;

import java.util.LinkedHashMap;
import java.util.SequencedCollection;
import java.util.SequencedMap;

public class Schema {

    private final TableName tableName;
    private final SequencedMap<FieldName, FieldInf> fields;

    public Schema(TableName tableName) {
        this.tableName = tableName;
        this.fields = new LinkedHashMap<>();
    }

    public TableName tableName() {
        return tableName;
    }

    public void addField(FieldName name, int type, int length) {
        fields.put(name, new FieldInf(type, length));
    }

    public void addIntField(FieldName name) {
        addField(name, java.sql.Types.INTEGER, 0);
    }

    public void addStringField(FieldName name, int length) {
        addField(name, java.sql.Types.VARCHAR, length);
    }

    public void add(FieldName name, Schema schema) {
        addField(name, schema.type(name), schema.length(name));
    }

    public void addAll(Schema schema) {
        for (FieldName name : schema.fields()) {
            add(name, schema);
        }
    }

    public SequencedCollection<FieldName> fields() {
        return fields.sequencedKeySet();
    }

    public FieldName get(int index) {
        return fields.sequencedKeySet().stream().skip(index).findFirst().orElseThrow();
    }

    public boolean hasField(FieldName name) {
        return fields.containsKey(name);
    }

    public int type(FieldName name) {
        return fields.get(name).type;
    }

    public int length(FieldName name) {
        return fields.get(name).length;
    }

    private record FieldInf(int type, int length) { }

}
