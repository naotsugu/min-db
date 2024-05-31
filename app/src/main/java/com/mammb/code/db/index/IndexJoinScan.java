package com.mammb.code.db.index;

import com.mammb.code.db.Index;
import com.mammb.code.db.Table;
import com.mammb.code.db.lang.DataBox;
import com.mammb.code.db.lang.FieldName;
import com.mammb.code.db.query.Scan;

public class IndexJoinScan implements Scan {
    private Scan lhs;
    private Index idx;
    private FieldName joinField;
    private Table rhs;

    public IndexJoinScan(Scan lhs, Index idx, FieldName joinField, Table rhs) {
        this.lhs = lhs;
        this.idx = idx;
        this.joinField = joinField;
        this.rhs = rhs;
        beforeFirst();
    }

    @Override
    public void beforeFirst() {
        lhs.beforeFirst();
        lhs.next();
        resetIndex();
    }

    @Override
    public boolean next() {
        while (true) {
            if (idx.next()) {
                rhs.moveToRid(idx.getDataRid());
                return true;
            }
            if (!lhs.next()) {
                return false;
            }
            resetIndex();
        }
    }

    @Override
    public int getInt(FieldName fieldName) {
        return rhs.hasField(fieldName)
            ? rhs.getInt(fieldName)
            : lhs.getInt(fieldName);

    }

    @Override
    public String getString(FieldName fieldName) {
        return rhs.hasField(fieldName)
            ? rhs.getString(fieldName)
            : lhs.getString(fieldName);
    }

    @Override
    public DataBox<?> getVal(FieldName fieldName) {
        return rhs.hasField(fieldName)
            ? rhs.getVal(fieldName)
            : lhs.getVal(fieldName);

    }

    @Override
    public boolean hasField(FieldName fieldName) {
        return rhs.hasField(fieldName) || lhs.hasField(fieldName);
    }

    @Override
    public void close() {
        lhs.close();
        idx.close();
        rhs.close();
    }

    private void resetIndex() {
        idx.beforeFirst(lhs.getVal(joinField));
    }
}
