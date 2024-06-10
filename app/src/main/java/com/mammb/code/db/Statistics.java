package com.mammb.code.db;

import com.mammb.code.db.lang.FieldName;
import com.mammb.code.db.lang.TableName;
import java.util.HashMap;
import java.util.Map;

public class Statistics {
    private final Catalog catalog;
    private final Map<TableName, Stat> tableStats = new HashMap<>();
    private int numCalls = 0;

    public Statistics(Catalog catalog) {
        this.catalog = catalog;
    }

    public void init(Transaction tx) {
        refreshStatistics(tx);
    }

    public synchronized Stat getStat(TableName tableName, Layout layout, Transaction tx) {
        numCalls++;
        if (numCalls > 100) {
            refreshStatistics(tx);
        }
        return tableStats.computeIfAbsent(tableName, t -> calcTableStats(tableName, layout, tx));
    }

    private synchronized void refreshStatistics(Transaction tx) {
        tableStats.clear();
        numCalls = 0;
        Table cat = new Table(tx, Catalog.Tab.TABLE_CAT, Catalog.Tab.layout);
        while (cat.next()) {
            TableName tableName = TableName.of(cat.getString(Catalog.Tab.TABLE_NAME));
            Layout layout = catalog.getLayout(tableName, tx);
            Stat si = calcTableStats(tableName, layout, tx);
            tableStats.put(tableName, si);
        }
        cat.close();
    }

    private synchronized Stat calcTableStats(TableName tableName, Layout layout, Transaction tx) {
        int numRecs = 0;
        int numBlocks = 0;
        Table ts = new Table(tx, tableName, layout);
        while (ts.next()) {
            numRecs++;
            numBlocks = ts.getRid().blockNum() + 1;
        }
        ts.close();
        return new Stat(numBlocks, numRecs);
    }

    public record Stat(int numBlocks, int numRecs) {
        public int distinctValues(FieldName fieldName) {
            return 1 + (numRecs / 3);
        }
    }
}
