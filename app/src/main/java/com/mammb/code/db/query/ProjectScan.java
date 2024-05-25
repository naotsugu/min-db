package com.mammb.code.db.query;

import com.mammb.code.db.lang.DataBox;
import com.mammb.code.db.lang.FieldName;
import java.util.List;

public class ProjectScan implements Scan {
    private Scan s;
    private List<FieldName> fieldlist;

    public ProjectScan(Scan s, List<FieldName> fieldlist) {
        this.s = s;
        this.fieldlist = fieldlist;
    }

    @Override
    public void beforeFirst() {
        s.beforeFirst();
    }

    @Override
    public boolean next() {
        return s.next();
    }

    @Override
    public int getInt(FieldName fieldName) {
        if (hasField(fieldName)) {
            return s.getInt(fieldName);
        }
        throw new RuntimeException("field " + fieldName.val() + " not found.");
    }

    @Override
    public String getString(FieldName fieldName) {
        if (hasField(fieldName)) {
            return s.getString(fieldName);
        }
        throw new RuntimeException("field " + fieldName.val() + " not found.");
    }

    @Override
    public DataBox<?> getVal(FieldName fieldName) {
        if (hasField(fieldName)) {
            return s.getVal(fieldName);
        }
        throw new RuntimeException("field " + fieldName.val() + " not found.");
    }

    @Override
    public boolean hasField(FieldName fieldName) {
        return fieldlist.contains(fieldName);
    }

    @Override
    public void close() {
        s.close();
    }
}
