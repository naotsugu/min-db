package com.mammb.code.db.catalog;

import com.mammb.code.db.FieldName;
import com.mammb.code.db.Layout;
import com.mammb.code.db.Schema;
import com.mammb.code.db.TableName;
import com.mammb.code.db.Transaction;

public class Catalog {

    private static Layout tableCatalogLayout = tableCatalogLayout();
    private static Layout fieldCatalogLayout = fieldCatalogLayout();

    private static Layout tableCatalogLayout() {
        Schema schema = new Schema(new TableName("tblcat"));
        schema.addStringField(new FieldName("tblname"), 32);
        schema.addIntField(new FieldName("slotsize"));
        return new Layout(schema);
    }

    private static Layout fieldCatalogLayout() {
        Schema schema = new Schema(new TableName("fldcat"));
        schema.addStringField(new FieldName("tblname"), 32);
        schema.addStringField(new FieldName("fldname"), 32);
        schema.addIntField(new FieldName("type"));
        schema.addIntField(new FieldName("length"));
        schema.addIntField(new FieldName("offset"));
        return new Layout(schema);
    }

    private void create(Layout layout, Transaction tx) {

    }
}
