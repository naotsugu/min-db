package com.mammb.code.db.plan;

import com.mammb.code.db.Schema;
import com.mammb.code.db.lang.FieldName;
import com.mammb.code.db.query.ProjectScan;
import com.mammb.code.db.query.Scan;
import java.util.SequencedCollection;

public class ProjectPlan implements Plan {
    private Plan p;
    private Schema schema;

    public ProjectPlan(Plan p, SequencedCollection<FieldName> fieldList) {
        this.p = p;
        schema = new Schema(null);
        for (FieldName fieldName : fieldList) {
            schema.add(fieldName, p.schema());
        }
    }

    @Override
    public Scan open() {
        Scan s = p.open();
        return new ProjectScan(s, schema.fields());
    }

    @Override
    public int blocksAccessed() {
        return p.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        return p.recordsOutput();
    }

    @Override
    public int distinctValues(FieldName fieldName) {
        return p.distinctValues(fieldName);
    }

    @Override
    public Schema schema() {
        return schema;
    }
}
