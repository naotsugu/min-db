package com.mammb.code.db;

import com.mammb.code.db.lang.FieldName;
import com.mammb.code.db.lang.IdxName;
import com.mammb.code.db.lang.TableName;
import java.util.Map;

public class Metadata {
    private static final Catalog catalog = new Catalog();
    private static final IndexCatalog indexCatalog = new IndexCatalog(catalog);

    public Metadata() {
    }

    public void init(Transaction tx) {
        catalog.init(tx);
        indexCatalog.init(tx);
    }

    public void createTable(TableName name, Schema schema, Transaction tx) {
        catalog.createTable(name, schema, tx);
    }

    public Layout getLayout(TableName name, Transaction tx) {
        return catalog.getLayout(name, tx);
    }

    public void createIndex(IdxName idxName, TableName tableName, FieldName fieldName, Transaction tx) {
        indexCatalog.createIndex(idxName, tableName, fieldName, tx);
    }

    public Map<FieldName, IndexStat> getIndexInfo(TableName tableName, Transaction tx) {
        return indexCatalog.getIndexInfo(tableName, tx);
    }

    public Statistics.Stat getStatInfo(TableName tableName, Layout layout, Transaction tx) {
        return indexCatalog.getStatInfo(tableName, layout, tx);
    }

}
