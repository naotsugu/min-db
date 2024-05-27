package com.mammb.code.db.plan;

import com.mammb.code.db.Schema;
import com.mammb.code.db.lang.FieldName;
import com.mammb.code.db.query.ProductScan;
import com.mammb.code.db.query.Scan;

public class ProductPlan implements Plan {
    private Plan p1, p2;
    private Schema schema;

    public ProductPlan(Plan p1, Plan p2) {
        this.p1 = p1;
        this.p2 = p2;
        this.schema = new Schema(null);
        this.schema.addAll(p1.schema());
        this.schema.addAll(p2.schema());
    }

    @Override
    public Scan open() {
        Scan s1 = p1.open();
        Scan s2 = p2.open();
        return new ProductScan(s1, s2);
    }

    @Override
    public int blocksAccessed() {
        return p1.blocksAccessed() + (p1.recordsOutput() * p2.blocksAccessed());
    }

    @Override
    public int recordsOutput() {
        return p1.recordsOutput() * p2.recordsOutput();
    }

    @Override
    public int distinctValues(FieldName fieldName) {
        if (p1.schema().hasField(fieldName)) {
            return p1.distinctValues(fieldName);
        } else {
            return p2.distinctValues(fieldName);
        }
    }

    @Override
    public Schema schema() {
        return schema;
    }
}
