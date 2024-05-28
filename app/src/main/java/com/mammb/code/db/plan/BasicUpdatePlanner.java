package com.mammb.code.db.plan;

import com.mammb.code.db.Metadata;
import com.mammb.code.db.Transaction;
import com.mammb.code.db.lang.DataBox;
import com.mammb.code.db.lang.FieldName;
import com.mammb.code.db.query.CreateIndexData;
import com.mammb.code.db.query.CreateTableData;
import com.mammb.code.db.query.DeleteData;
import com.mammb.code.db.query.InsertData;
import com.mammb.code.db.query.ModifyData;
import com.mammb.code.db.query.UpdateScan;
import java.util.Iterator;

public class BasicUpdatePlanner implements UpdatePlanner {
    private final Metadata metadata;

    public BasicUpdatePlanner(Metadata metadata) {
        this.metadata = metadata;
    }

    @Override
    public int executeInsert(InsertData data, Transaction tx) {
        Plan p = new TablePlan(tx, data.tableName(), metadata);
        UpdateScan us = (UpdateScan) p.open();
        us.insert();
        Iterator<DataBox<?>> iter = data.vals().iterator();
        for (FieldName fieldName : data.fields()) {
            DataBox<?> val = iter.next();
            us.setVal(fieldName, val);
        }
        us.close();
        return 1;

    }

    @Override
    public int executeDelete(DeleteData data, Transaction tx) {
        Plan p = new TablePlan(tx, data.tableName(), metadata);
        p = new SelectPlan(p, data.predicate());
        UpdateScan us = (UpdateScan) p.open();
        int count = 0;
        while(us.next()) {
            us.delete();
            count++;
        }
        us.close();
        return count;

    }

    @Override
    public int executeModify(ModifyData data, Transaction tx) {
        Plan p = new TablePlan(tx, data.tableName(), metadata);
        p = new SelectPlan(p, data.predicate());
        UpdateScan us = (UpdateScan) p.open();
        int count = 0;
        while(us.next()) {
            DataBox<?> val = data.newValue().evaluate(us);
            us.setVal(data.targetField(), val);
            count++;
        }
        us.close();
        return count;
    }

    @Override
    public int executeCreateTable(CreateTableData data, Transaction tx) {
        metadata.createTable(data.tableName(), data.schema(), tx);
        return 0;

    }

    @Override
    public int executeCreateIndex(CreateIndexData data, Transaction tx) {
        metadata.createIndex(data.indexName(), data.tableName(), data.fieldName(), tx);
        return 0;

    }
}
