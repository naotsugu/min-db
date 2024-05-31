package com.mammb.code.db.query;

import com.mammb.code.db.RId;
import com.mammb.code.db.lang.DataBox;
import com.mammb.code.db.lang.FieldName;

public class SelectScan implements UpdateScan {
    private Scan scan;
    private Predicate predicate;

    public SelectScan(Scan scan, Predicate predicate) {
        this.scan = scan;
        this.predicate = predicate;
    }

    @Override
    public void beforeFirst() {
        scan.beforeFirst();
    }

    @Override
    public boolean next() {
        while (scan.next()) {
            if (predicate.isSatisfied(scan)) {
                return true;
            }
        }
        return false;

    }

    @Override
    public int getInt(FieldName fieldName) {
        return scan.getInt(fieldName);
    }

    @Override
    public String getString(FieldName fieldName) {
        return scan.getString(fieldName);
    }

    @Override
    public DataBox<?> getVal(FieldName fieldName) {
        return scan.getVal(fieldName);
    }

    @Override
    public boolean hasField(FieldName fieldName) {
        return scan.hasField(fieldName);
    }

    @Override
    public void close() {
        scan.close();
    }

    @Override
    public void setVal(FieldName fieldName, DataBox<?> val) {
        UpdateScan us = (UpdateScan) scan;
        us.setVal(fieldName, val);
    }

    @Override
    public void setInt(FieldName fieldName, int val) {
        UpdateScan us = (UpdateScan) scan;
        us.setInt(fieldName, val);
    }

    @Override
    public void setString(FieldName fieldName, String val) {
        UpdateScan us = (UpdateScan) scan;
        us.setString(fieldName, val);
    }

    @Override
    public void insert() {
        UpdateScan us = (UpdateScan) scan;
        us.insert();
    }

    @Override
    public void delete() {
        UpdateScan us = (UpdateScan) scan;
        us.delete();
    }

    @Override
    public RId getRid() {
        UpdateScan us = (UpdateScan) scan;
        return us.getRid();
    }

    @Override
    public void moveToRid(RId rid) {
        UpdateScan us = (UpdateScan) scan;
        us.moveToRid(rid);
    }

}
