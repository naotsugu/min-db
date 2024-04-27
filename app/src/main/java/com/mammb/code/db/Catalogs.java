package com.mammb.code.db;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class Catalogs {

    private static final TableName TABLE_CAT = new TableName("tblcat");
    private static final FieldName TABLE_NAME = new FieldName("tblname");
    private static final FieldName SLOT_SIZE = new FieldName("slotsize");

    private static final TableName FIELD_CAT = new TableName("fldcat");
    private static final FieldName FIELD_NAME = new FieldName("fldname");
    private static final FieldName TYPE = new FieldName("type");
    private static final FieldName LENGTH = new FieldName("length");
    private static final FieldName OFFSET = new FieldName("offset");

    private static final Layout tableCatalogLayout;
    private static final Layout fieldCatalogLayout;
    static {
        var schema = new Schema(TABLE_CAT);
        schema.addStringField(TABLE_NAME, 32);
        schema.addIntField(SLOT_SIZE);
        tableCatalogLayout = new Layout(schema);
    }
    static {
        var schema = new Schema(FIELD_CAT);
        schema.addStringField(TABLE_NAME, 32);
        schema.addStringField(FIELD_NAME, 32);
        schema.addIntField(TYPE);
        schema.addIntField(LENGTH);
        schema.addIntField(OFFSET);
        fieldCatalogLayout = new Layout(schema);
    }


    private void create(Transaction tx) {
        var table = new Table(tx, tableCatalogLayout);
        Schema sc = table.schema();
        table.insert();
        table.setString(sc.get(0), sc.tableName().value());
        table.setInt(sc.get(1), table.layout().slotSize());
        table.close();
        createFieldCatalog(sc.tableName(), tx);
    }

    private void createFieldCatalog(TableName tableName, Transaction tx) {
        var table = new Table(tx, fieldCatalogLayout);
        Schema sc = table.schema();
        for (FieldName fieldName : sc.fields()) {
            table.insert();
            table.setString(sc.get(0), tableName.value());
            table.setString(sc.get(1), fieldName.value());
            table.setInt(sc.get(2), sc.type(fieldName));
            table.setInt(sc.get(3), sc.length(fieldName));
            table.setInt(sc.get(4), table.layout().offset(fieldName));
        }
        table.close();
    }

    public Layout getLayout(TableName name, Transaction tx) {
        int size = -1;
        Table tableCat = new Table(tx, tableCatalogLayout);
        while (tableCat.next()) {
            if (Objects.equals(tableCat.getString(TABLE_NAME), name.value())) {
                size = tableCat.getInt(SLOT_SIZE);
                break;
            }
        }
        tableCat.close();

        Schema sch = new Schema(name);
        Map<FieldName, Integer> offsets = new HashMap<>();
        Table fieldCat = new Table(tx, fieldCatalogLayout);
        while (fieldCat.next()) {
            if (Objects.equals(fieldCat.getString(TABLE_NAME), name.value())) {
                FieldName field = new FieldName(fieldCat.getString(FIELD_NAME));
                int type = fieldCat.getInt(TYPE);
                int len = fieldCat.getInt(LENGTH);
                int offset = fieldCat.getInt(OFFSET);
                offsets.put(field, offset);
                sch.addField(field, type, len);
            }
        }
        fieldCat.close();
        return new Layout(sch, offsets, size);
    }

}
