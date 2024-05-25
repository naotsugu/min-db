package com.mammb.code.db.query;

import com.mammb.code.db.lang.DataBox;
import com.mammb.code.db.lang.FieldName;
import simpledb.record.RID;

public interface UpdateScan extends Scan {
    void setVal(FieldName fieldName, DataBox<?> val);
    void setInt(FieldName fieldName, int val);
    void setString(FieldName fieldName, String val);
    void insert();
    void delete();
    RID getRid();
    void moveToRid(RID rid);
}
