package com.mammb.code.db;

import java.util.HashMap;

public class Catalogs {

    private static final TableName TABLE_CAT = new TableName("table_catalog");
    private static final FieldName TABLE_NAME = new FieldName("table_name");
    private static final FieldName SLOT_SIZE = new FieldName("slot_size");

    private static final TableName FIELD_CAT = new TableName("field_catalog");
    private static final FieldName FIELD_NAME = new FieldName("field_name");
    private static final FieldName TYPE = new FieldName("type");
    private static final FieldName LENGTH = new FieldName("length");
    private static final FieldName OFFSET = new FieldName("offset");

    private static final Layout tableCatalogLayout = new Layout(new Schema(TABLE_CAT));
    private static final Layout fieldCatalogLayout = new Layout(new Schema(FIELD_CAT));
    static {
        tableCatalogLayout.schema().addStringField(TABLE_NAME, 32);
        tableCatalogLayout.schema().addIntField(SLOT_SIZE);
        fieldCatalogLayout.schema().addStringField(TABLE_NAME, 32);
        fieldCatalogLayout.schema().addStringField(FIELD_NAME, 32);
        fieldCatalogLayout.schema().addIntField(TYPE);
        fieldCatalogLayout.schema().addIntField(LENGTH);
        fieldCatalogLayout.schema().addIntField(OFFSET);
    }

    private void create(Transaction tx) {
        var table = new Table(tx, tableCatalogLayout);
        Schema sc = table.schema();
        table.insert();
        table.setString(sc.get(0), sc.tableName().val());
        table.setInt(sc.get(1), table.layout().slotSize());
        table.close();
        createFieldCatalog(sc.tableName(), tx);
    }

    private void createFieldCatalog(TableName tableName, Transaction tx) {
        var table = new Table(tx, fieldCatalogLayout);
        Schema sc = table.schema();
        for (FieldName fieldName : sc.fields()) {
            table.insert();
            table.setString(sc.get(0), tableName.val());
            table.setString(sc.get(1), fieldName.val());
            table.setInt(sc.get(2), sc.type(fieldName));
            table.setInt(sc.get(3), sc.length(fieldName));
            table.setInt(sc.get(4), table.layout().offset(fieldName));
        }
        table.close();
    }

    public Layout getLayout(TableName name, Transaction tx) {
        int size = -1;
        var tCat = new Table(tx, tableCatalogLayout);
        while (tCat.next()) {
            if (tCat.getString(TABLE_NAME).equals(name.val())) {
                size = tCat.getInt(SLOT_SIZE);
                break;
            }
        }
        tCat.close();

        var schema = new Schema(name);
        var offsets = new HashMap<FieldName, Integer>();
        var fCat = new Table(tx, fieldCatalogLayout);
        while (fCat.next()) {
            if (fCat.getString(TABLE_NAME).equals(name.val())) {
                FieldName field = new FieldName(fCat.getString(FIELD_NAME));
                int type = fCat.getInt(TYPE);
                int len = fCat.getInt(LENGTH);
                int offset = fCat.getInt(OFFSET);
                offsets.put(field, offset);
                schema.addField(field, type, len);
            }
        }
        fCat.close();

        return new Layout(schema, offsets, size);
    }

}
