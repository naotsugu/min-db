package com.mammb.code.db.plan;

import com.mammb.code.db.Schema;
import com.mammb.code.db.lang.FieldName;
import com.mammb.code.db.query.Scan;

public class OptimizedProductPlan implements Plan {
    private Plan bastPlan;

    public OptimizedProductPlan(Plan p1, Plan p2) {
        Plan prod1 = new ProductPlan(p1, p2);
        Plan prod2 = new ProductPlan(p2, p1);
        int b1 = prod1.blocksAccessed();
        int b2 = prod2.blocksAccessed();
        bastPlan = (b1 < b2) ? prod1 : prod2;
    }

    @Override
    public Scan open() {
        return bastPlan.open();
    }

    @Override
    public int blocksAccessed() {
        return bastPlan.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        return bastPlan.recordsOutput();
    }

    @Override
    public int distinctValues(FieldName fieldName) {
        return bastPlan.distinctValues(fieldName);
    }

    @Override
    public Schema schema() {
        return bastPlan.schema();
    }
}
