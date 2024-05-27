package com.mammb.code.db.plan;

import com.mammb.code.db.Schema;
import com.mammb.code.db.lang.FieldName;
import com.mammb.code.db.query.Predicate;
import com.mammb.code.db.query.Scan;
import com.mammb.code.db.query.SelectScan;

public class SelectPlan implements Plan {

    private Plan plan;
    private Predicate predicate;

    public SelectPlan(Plan plan, Predicate predicate) {
        this.plan = plan;
        this.predicate = predicate;
    }

    @Override
    public Scan open() {
        Scan s = plan.open();
        return new SelectScan(s, predicate);
    }

    @Override
    public int blocksAccessed() {
        return plan.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        return plan.recordsOutput() / predicate.reductionFactor(plan);
    }

    @Override
    public int distinctValues(FieldName fieldName) {
        if (predicate.equatesWithConstant(fieldName) != null)
            return 1;
        else {
            FieldName fieldName2 = predicate.equatesWithField(fieldName);
            if (fieldName2 != null) {
                return Math.min(plan.distinctValues(fieldName), plan.distinctValues(fieldName2));
            } else {
                return plan.distinctValues(fieldName);
            }
        }
    }

    @Override
    public Schema schema() {
        return plan.schema();
    }

}
