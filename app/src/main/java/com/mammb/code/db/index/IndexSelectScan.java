package com.mammb.code.db.index;

import com.mammb.code.db.Index;
import com.mammb.code.db.RId;
import com.mammb.code.db.Table;
import com.mammb.code.db.lang.DataBox;
import com.mammb.code.db.lang.FieldName;
import com.mammb.code.db.query.Scan;

public class IndexSelectScan implements Scan {
    private Table ts;
    private Index idx;
    private DataBox<?> val;

    public IndexSelectScan(Table ts, Index idx, DataBox<?> val) {
        this.ts = ts;
        this.idx = idx;
        this.val = val;
        beforeFirst();
    }

    @Override
    public void beforeFirst() {
        idx.beforeFirst(val);
    }

    @Override
    public boolean next() {
        boolean ok = idx.next();
        if (ok) {
            RId rid = idx.getDataRid();
            ts.moveToRid(rid);
        }
        return ok;
    }

    @Override
    public int getInt(FieldName fieldName) {
        return ts.getInt(fieldName);
    }

    @Override
    public String getString(FieldName fieldName) {
        return ts.getString(fieldName);
    }

    @Override
    public DataBox<?> getVal(FieldName fieldName) {
        return ts.getVal(fieldName);
    }

    @Override
    public boolean hasField(FieldName fieldName) {
        return ts.hasField(fieldName);
    }

    @Override
    public void close() {
        idx.close();
        ts.close();
    }

}
