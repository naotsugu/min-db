package com.mammb.code.db;

import com.mammb.code.db.lang.FieldName;
import com.mammb.code.db.lang.TableName;
import java.util.HashMap;
import java.util.Map;

public class Statistics {
    private Catalog catalog;
    private Map<TableName, Stat> tableStats = new HashMap<>();
    private int numCalls = 0;

    public Statistics(Catalog catalog) {
        this.catalog = catalog;
    }

    public synchronized Stat getStat(Layout layout, Transaction tx) {
        numCalls++;
        if (numCalls > 100) {
            refreshStatistics(tx);
        }
        return tableStats.computeIfAbsent(
            layout.schema().tableName(), t -> calcTableStats(layout, tx));
    }

    private synchronized void refreshStatistics(Transaction tx) {
        tableStats.clear();
        numCalls = 0;
        Table cat = new Table(tx, Catalog.Tab.layout);
        while (cat.next()) {
            TableName tableName = TableName.of(cat.getString(Catalog.Tab.TABLE_NAME));
            Layout layout = catalog.getLayout(tableName, tx);
            Stat si = calcTableStats(layout, tx);
            tableStats.put(tableName, si);
        }
        cat.close();
    }

    private synchronized Stat calcTableStats(Layout layout, Transaction tx) {
        int numRecs = 0;
        int numBlocks = 0;
        Table ts = new Table(tx, layout);
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
