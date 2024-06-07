package com.mammb.code.db.plan;

import com.mammb.code.db.Schema;
import com.mammb.code.db.lang.FieldName;
import com.mammb.code.db.query.ProjectScan;
import com.mammb.code.db.query.Scan;
import java.util.SequencedCollection;

public class ProjectPlan implements Plan {
    private final Plan plan;
    private final Schema schema;

    public ProjectPlan(Plan plan, SequencedCollection<FieldName> fieldList) {
        this.plan = plan;
        schema = new Schema(null);
        for (FieldName fieldName : fieldList) {
            schema.add(fieldName, plan.schema());
        }
    }

    @Override
    public Scan open() {
        Scan s = plan.open();
        return new ProjectScan(s, schema.fields());
    }

    @Override
    public int blocksAccessed() {
        return plan.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        return plan.recordsOutput();
    }

    @Override
    public int distinctValues(FieldName fieldName) {
        return plan.distinctValues(fieldName);
    }

    @Override
    public Schema schema() {
        return schema;
    }
}
