package com.mammb.code.db.plan;

import com.mammb.code.db.Layout;
import com.mammb.code.db.Metadata;
import com.mammb.code.db.Schema;
import com.mammb.code.db.Statistics;
import com.mammb.code.db.Table;
import com.mammb.code.db.Transaction;
import com.mammb.code.db.lang.FieldName;
import com.mammb.code.db.lang.TableName;
import com.mammb.code.db.query.Scan;

public class TablePlan implements Plan {

    private TableName tableName;
    private final Transaction tx;
    private final Layout layout;
    private final Statistics.Stat si;

    public TablePlan(Transaction tx, TableName tableName, Metadata md) {
        this.tableName = tableName;
        this.tx = tx;
        layout = md.getLayout(tableName, tx);
        si = md.getStatInfo(tableName, layout, tx);
    }

    @Override
    public Scan open() {
        return new Table(tx, tableName, layout);
    }

    @Override
    public int blocksAccessed() {
        return si.numBlocks();
    }

    @Override
    public int recordsOutput() {
        return si.numRecs();
    }

    @Override
    public int distinctValues(FieldName fieldName) {
        return si.distinctValues(fieldName);
    }

    @Override
    public Schema schema() {
        return layout.schema();
    }
}
