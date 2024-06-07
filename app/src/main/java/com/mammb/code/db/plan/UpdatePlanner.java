package com.mammb.code.db.plan;

import com.mammb.code.db.Transaction;
import com.mammb.code.db.query.Parser.CreateIndexData;
import com.mammb.code.db.query.Parser.CreateTableData;
import com.mammb.code.db.query.Parser.DeleteData;
import com.mammb.code.db.query.Parser.InsertData;
import com.mammb.code.db.query.Parser.ModifyData;

public interface UpdatePlanner {
    int executeInsert(InsertData data, Transaction tx);
    int executeDelete(DeleteData data, Transaction tx);
    int executeModify(ModifyData data, Transaction tx);
    int executeCreateTable(CreateTableData data, Transaction tx);
    int executeCreateIndex(CreateIndexData data, Transaction tx);
}
