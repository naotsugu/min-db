package com.mammb.code.db.index;

import com.mammb.code.db.Index;
import com.mammb.code.db.IndexStat;
import com.mammb.code.db.Schema;
import com.mammb.code.db.Table;
import com.mammb.code.db.lang.FieldName;
import com.mammb.code.db.plan.Plan;
import com.mammb.code.db.query.Scan;

public class IndexJoinPlan implements Plan {
    private Plan p1, p2;
    private IndexStat ii;
    private FieldName joinField;
    private Schema sch = new Schema();

    public IndexJoinPlan(Plan p1, Plan p2, IndexStat ii, FieldName joinField) {
        this.p1 = p1;
        this.p2 = p2;
        this.ii = ii;
        this.joinField = joinField;
        sch.addAll(p1.schema());
        sch.addAll(p2.schema());
    }

    @Override
    public Scan open() {
        Scan s = p1.open();
        // throws an exception if p2 is not a tableplan
        Table ts = (Table) p2.open();
        Index idx = ii.open();
        return new IndexJoinScan(s, idx, joinField, ts);
    }

    @Override
    public int blocksAccessed() {
        return p1.blocksAccessed()
            + (p1.recordsOutput() * ii.blocksAccessed())
            + recordsOutput();
    }

    @Override
    public int recordsOutput() {
        return p1.recordsOutput() * ii.recordsOutput();
    }

    @Override
    public int distinctValues(FieldName fieldName) {
        return p1.schema().hasField(fieldName)
            ?  p1.distinctValues(fieldName)
            : p2.distinctValues(fieldName);

    }

    @Override
    public Schema schema() {
        return sch;
    }
}
