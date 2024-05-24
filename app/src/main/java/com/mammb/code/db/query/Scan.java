package com.mammb.code.db.query;

import com.mammb.code.db.lang.DataBox;
import com.mammb.code.db.lang.FieldName;

public interface Scan {
    void beforeFirst();
    boolean next();
    int getInt(FieldName fieldName);
    String getString(FieldName fieldName);
    DataBox<?> getVal(FieldName fieldName);
    boolean hasField(FieldName fieldName);
    void close();
}
