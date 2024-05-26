package com.mammb.code.db.plan;

import com.mammb.code.db.Schema;
import com.mammb.code.db.lang.FieldName;
import com.mammb.code.db.query.Scan;

public interface Plan {
    Scan open();
    int blocksAccessed();
    int recordsOutput();
    int distinctValues(FieldName fieldName);
    Schema schema();
}
