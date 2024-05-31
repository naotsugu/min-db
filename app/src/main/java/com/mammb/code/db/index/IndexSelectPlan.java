package com.mammb.code.db.index;

import com.mammb.code.db.Index;
import com.mammb.code.db.IndexStat;
import com.mammb.code.db.Schema;
import com.mammb.code.db.Table;
import com.mammb.code.db.lang.DataBox;
import com.mammb.code.db.lang.FieldName;
import com.mammb.code.db.plan.Plan;
import com.mammb.code.db.query.Scan;

public class IndexSelectPlan implements Plan {
    private Plan p;
    private IndexStat ii;
    private DataBox<?> val;

    public IndexSelectPlan(Plan p, IndexStat ii, DataBox<?> val) {
        this.p = p;
        this.ii = ii;
        this.val = val;
    }

    @Override
    public Scan open() {
        // throws an exception if p is not a tableplan.
        Table ts = (Table) p.open();
        Index idx = ii.open();
        return new IndexSelectScan(ts, idx, val);
    }

    @Override
    public int blocksAccessed() {
        return ii.blocksAccessed() + recordsOutput();
    }

    @Override
    public int recordsOutput() {
        return ii.recordsOutput();
    }

    @Override
    public int distinctValues(FieldName fieldName) {
        return ii.distinctValues(fieldName);
    }

    @Override
    public Schema schema() {
        return p.schema();
    }
}
