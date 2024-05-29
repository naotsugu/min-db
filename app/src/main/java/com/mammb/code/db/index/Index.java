package com.mammb.code.db.index;

import com.mammb.code.db.RId;
import com.mammb.code.db.lang.DataBox;

public interface Index {
    void beforeFirst(DataBox<?> searchKey);
    boolean next();
    RId getRid();
    void insert(DataBox<?> val, RId rid);
    void delete(DataBox<?> val, RId rid);
    void close();
}
