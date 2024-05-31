package com.mammb.code.db;

import com.mammb.code.db.lang.DataBox;
import com.mammb.code.db.lang.FieldName;
import com.mammb.code.db.lang.IdxName;
import com.mammb.code.db.lang.TableName;

public interface Index {
    boolean next();
    RId getDataRid();
    void beforeFirst(DataBox<?> searchKey);
    void insert(DataBox<?> val, RId rid);
    void delete(DataBox<?> val, RId rid);
    void close();
}
