package com.mammb.code.db.catalog;

import com.mammb.code.db.FieldName;
import com.mammb.code.db.Layout;
import com.mammb.code.db.Schema;
import com.mammb.code.db.Table;
import com.mammb.code.db.TableName;
import com.mammb.code.db.Transaction;

public class Catalog {

    private static final Layout tableCatalogLayout;
    private static final Layout fieldCatalogLayout;

    static {
        var schema = new Schema(new TableName("tblcat"));
        schema.addStringField(new FieldName("tblname"), 32);
        schema.addIntField(new FieldName("slotsize"));
        tableCatalogLayout = new Layout(schema);
    }
    static {
        var schema = new Schema(new TableName("fldcat"));
        schema.addStringField(new FieldName("tblname"), 32);
        schema.addStringField(new FieldName("fldname"), 32);
        schema.addIntField(new FieldName("type"));
        schema.addIntField(new FieldName("length"));
        schema.addIntField(new FieldName("offset"));
        fieldCatalogLayout = new Layout(schema);
    }

    private void create(Transaction tx) {
        var catalog = new Table(tx, tableCatalogLayout);
        Schema sc = catalog.schema();
        catalog.insert();
        catalog.setString(sc.get(0), sc.tableName().value());
        catalog.setInt(sc.get(1), catalog.layout().slotSize());
        catalog.close();

        create(sc.tableName(), tx);
    }

    private void create(TableName tableName, Transaction tx) {
        var catalog = new Table(tx, fieldCatalogLayout);
        Schema sc = catalog.schema();
        for (FieldName fieldName : sc.fields()) {
            catalog.insert();
            catalog.setString(sc.get(0), tableName.value());
            catalog.setString(sc.get(1), fieldName.value());
            catalog.setInt(sc.get(2), sc.type(fieldName));
            catalog.setInt(sc.get(3), sc.length(fieldName));
            catalog.setInt(sc.get(4), catalog.layout().offset(fieldName));
        }
        catalog.close();
    }

}
