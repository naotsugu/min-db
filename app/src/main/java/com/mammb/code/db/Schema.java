package com.mammb.code.db;

import java.util.LinkedHashMap;
import java.util.SequencedCollection;
import java.util.SequencedMap;

public class Schema {

    private SequencedMap<String, Field> fields = new LinkedHashMap<>();

    public record Field(int type, int length) { }

    public void addField(String name, int type, int length) {
        fields.put(name, new Field(type, length));
    }

    public void addIntField(String name) {
        addField(name, java.sql.Types.INTEGER, 0);
    }

    public void addStringField(String name, int length) {
        addField(name, java.sql.Types.VARCHAR, length);
    }

    public void add(String name, Schema schema) {
        addField(name, schema.type(name), schema.length(name));
    }

    public void addAll(Schema schema) {
        for (String name : schema.fields()) {
            add(name, schema);
        }
    }

    public SequencedCollection<String> fields() {
        return fields.sequencedKeySet();
    }

    public boolean hasField(String name) {
        return fields.containsKey(name);
    }

    public int type(String name) {
        return fields.get(name).type;
    }

    public int length(String name) {
        return fields.get(name).length;
    }
}
