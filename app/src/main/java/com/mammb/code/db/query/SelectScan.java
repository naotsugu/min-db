package com.mammb.code.db.query;

import com.mammb.code.db.lang.DataBox;
import com.mammb.code.db.lang.FieldName;

public class SelectScan implements Scan {
    private Scan s;
    private Predicate pred;

    public SelectScan(Scan s, Predicate pred) {
        this.s = s;
        this.pred = pred;
    }

    @Override
    public void beforeFirst() {
        s.beforeFirst();
    }

    @Override
    public boolean next() {
        while (s.next()) {
            if (pred.isSatisfied(s)) {
                return true;
            }
        }
        return false;

    }

    @Override
    public int getInt(FieldName fieldName) {
        return s.getInt(fieldName);
    }

    @Override
    public String getString(FieldName fieldName) {
        return s.getString(fieldName);
    }

    @Override
    public DataBox<?> getVal(FieldName fieldName) {
        return s.getVal(fieldName);
    }

    @Override
    public boolean hasField(FieldName fieldName) {
        return s.hasField(fieldName);
    }

    @Override
    public void close() {
        s.close();
    }

}
