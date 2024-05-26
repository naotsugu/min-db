package com.mammb.code.db.plan;

import com.mammb.code.db.Transaction;
import com.mammb.code.db.query.CreateIndexData;
import com.mammb.code.db.query.CreateTableData;
import com.mammb.code.db.query.DeleteData;
import com.mammb.code.db.query.InsertData;
import com.mammb.code.db.query.ModifyData;

public interface UpdatePlanner {
    int executeInsert(InsertData data, Transaction tx);
    int executeDelete(DeleteData data, Transaction tx);
    int executeModify(ModifyData data, Transaction tx);
    int executeCreateTable(CreateTableData data, Transaction tx);
    int executeCreateIndex(CreateIndexData data, Transaction tx);
}
