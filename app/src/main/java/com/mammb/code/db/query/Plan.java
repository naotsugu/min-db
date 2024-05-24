package com.mammb.code.db.query;

import com.mammb.code.db.Schema;
import com.mammb.code.db.lang.FieldName;

public interface Plan {
    Scan open();
    int blocksAccessed();
    int recordsOutput();
    int distinctValues(FieldName fieldName);
    Schema schema();
}
