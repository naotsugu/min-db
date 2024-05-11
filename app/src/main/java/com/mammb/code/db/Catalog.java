package com.mammb.code.db;

import java.util.HashMap;

public class Catalog {

    public void create(Transaction tx) {
        createTable(Tab.layout.schema().tableName(), Tab.layout.schema(), tx);
        createTable(Fld.layout.schema().tableName(), Fld.layout.schema(), tx);
    }

    public void createTable(TableName name, Schema schema, Transaction tx) {
        Layout layout = new Layout(schema);

        var table = new Table(tx, Tab.layout);
        table.insert();
        table.setString(table.schema().get(0), name.val());
        table.setInt(table.schema().get(1), layout.slotSize());
        table.close();

        var field = new Table(tx, Fld.layout);
        for (FieldName fn : layout.schema().fields()) {
            field.insert();
            field.setString(field.schema().get(0), name.val());
            field.setString(field.schema().get(1), fn.val());
            field.setInt(field.schema().get(2), layout.schema().type(fn));
            field.setInt(field.schema().get(3), layout.schema().length(fn));
            field.setInt(field.schema().get(4), layout.offset(fn));
        }
        field.close();
    }

    public Layout getLayout(TableName name, Transaction tx) {
        int size = -1;
        var tCat = new Table(tx, Tab.layout);
        while (tCat.next()) {
            if (tCat.getString(Tab.TABLE_NAME).equals(name.val())) {
                size = tCat.getInt(Tab.SLOT_SIZE);
                break;
            }
        }
        tCat.close();

        var schema = new Schema(name);
        var offsets = new HashMap<FieldName, Integer>();
        var fCat = new Table(tx, Fld.layout);
        while (fCat.next()) {
            if (fCat.getString(Fld.TABLE_NAME).equals(name.val())) {
                FieldName field = new FieldName(fCat.getString(Fld.FIELD_NAME));
                int type = fCat.getInt(Fld.TYPE);
                int len = fCat.getInt(Fld.LENGTH);
                int offset = fCat.getInt(Fld.OFFSET);
                offsets.put(field, offset);
                schema.addField(field, type, len);
            }
        }
        fCat.close();

        return new Layout(schema, offsets, size);
    }


    private static class Tab {
        static final TableName TABLE_CAT = new TableName("table_catalog");
        static final FieldName TABLE_NAME = new FieldName("table_name");
        static final FieldName SLOT_SIZE = new FieldName("slot_size");
        static final Layout layout;
        static {
            Schema schema = new Schema(TABLE_CAT);
            schema.addStringField(TABLE_NAME, 32);
            schema.addIntField(SLOT_SIZE);
            layout = new Layout(schema);
        }
    }

    private static class Fld {
        static final TableName FIELD_CAT = new TableName("field_catalog");
        static final FieldName TABLE_NAME = new FieldName("table_name");
        static final FieldName FIELD_NAME = new FieldName("field_name");
        static final FieldName TYPE = new FieldName("type");
        static final FieldName LENGTH = new FieldName("length");
        static final FieldName OFFSET = new FieldName("offset");
        static final Layout layout;
        static {
            Schema schema = new Schema(FIELD_CAT);
            schema.addStringField(TABLE_NAME, 32);
            schema.addStringField(FIELD_NAME, 32);
            schema.addIntField(TYPE);
            schema.addIntField(LENGTH);
            schema.addIntField(OFFSET);
            layout = new Layout(schema);
        }
    }


    private static class Idx {
        static final TableName INDEX_CAT = new TableName("index_catalog");
        static final FieldName INDEX_NAME = new FieldName("index_name");
        static final FieldName TABLE_NAME = new FieldName("index_name");
        static final FieldName FIELD_NAME = new FieldName("field_name");
        static final Layout layout;
        static {
            Schema schema = new Schema(INDEX_CAT);
            schema.addStringField(INDEX_NAME, 32);
            schema.addStringField(TABLE_NAME, 32);
            schema.addStringField(FIELD_NAME, 32);
            layout = new Layout(schema);
        }
    }

}
