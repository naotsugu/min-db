package com.mammb.code.db;

import com.mammb.code.db.lang.*;
import java.util.*;
import static com.mammb.code.db.Catalog.*;

public class IndexCatalog {
    private Catalog catalog;
    private Statistics statistics;

    public IndexCatalog(Catalog catalog) {
        this.catalog = catalog;
        this.statistics = new Statistics(catalog);
    }

    public void init(Transaction tx) {
        statistics.init(tx);
    }

    public void createIndex(IdxName idxName, TableName tableName, FieldName fieldName, Transaction tx) {
        Table table = new Table(tx, Idx.layout);
        table.insert();
        table.setString(Idx.INDEX_NAME, idxName.val());
        table.setString(Idx.TABLE_NAME, tableName.val());
        table.setString(Idx.FIELD_NAME, fieldName.val());
        table.close();
    }

    public Map<FieldName, IndexStat> getIndexInfo(TableName tableName, Transaction tx) {
        Map<FieldName, IndexStat> map = new HashMap<>();
        Table table = new Table(tx, Idx.layout);
        while (table.next()) {
            if (table.getString(Idx.TABLE_NAME).equals(tableName.val())) {
                IdxName idxName = IdxName.of(table.getString(Idx.INDEX_NAME));
                FieldName fieldName = FieldName.of(table.getString(Idx.FIELD_NAME));
                Layout layout = catalog.getLayout(tableName, tx);
                Statistics.Stat stat = statistics.getStat(layout, tx);
                map.put(fieldName, new IndexStat(
                    idxName,
                    fieldName,
                    layout.schema(),
                    tx,
                    layout,
                    stat));
            }
        }
        return map;
    }

}
