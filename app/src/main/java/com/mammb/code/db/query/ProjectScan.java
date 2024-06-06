package com.mammb.code.db.query;

import com.mammb.code.db.lang.DataBox;
import com.mammb.code.db.lang.FieldName;
import java.util.SequencedCollection;

public class ProjectScan implements Scan {
    private final Scan scan;
    private final SequencedCollection<FieldName> fieldList;

    public ProjectScan(Scan scan, SequencedCollection<FieldName> fieldList) {
        this.scan = scan;
        this.fieldList = fieldList;
    }

    @Override
    public void beforeFirst() {
        scan.beforeFirst();
    }

    @Override
    public boolean next() {
        return scan.next();
    }

    @Override
    public int getInt(FieldName fieldName) {
        if (hasField(fieldName)) {
            return scan.getInt(fieldName);
        }
        throw new RuntimeException("field " + fieldName.val() + " not found.");
    }

    @Override
    public String getString(FieldName fieldName) {
        if (hasField(fieldName)) {
            return scan.getString(fieldName);
        }
        throw new RuntimeException("field " + fieldName.val() + " not found.");
    }

    @Override
    public DataBox<?> getVal(FieldName fieldName) {
        if (hasField(fieldName)) {
            return scan.getVal(fieldName);
        }
        throw new RuntimeException("field " + fieldName.val() + " not found.");
    }

    @Override
    public boolean hasField(FieldName fieldName) {
        return fieldList.contains(fieldName);
    }

    @Override
    public void close() {
        scan.close();
    }
}
