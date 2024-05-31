package com.mammb.code.db.index;

import com.mammb.code.db.Index;
import com.mammb.code.db.IndexStat;
import com.mammb.code.db.Metadata;
import com.mammb.code.db.RId;
import com.mammb.code.db.Transaction;
import com.mammb.code.db.lang.DataBox;
import com.mammb.code.db.lang.FieldName;
import com.mammb.code.db.lang.TableName;
import com.mammb.code.db.plan.Plan;
import com.mammb.code.db.plan.SelectPlan;
import com.mammb.code.db.plan.TablePlan;
import com.mammb.code.db.plan.UpdatePlanner;
import com.mammb.code.db.query.CreateIndexData;
import com.mammb.code.db.query.CreateTableData;
import com.mammb.code.db.query.DeleteData;
import com.mammb.code.db.query.InsertData;
import com.mammb.code.db.query.ModifyData;
import com.mammb.code.db.query.UpdateScan;
import java.util.Iterator;
import java.util.Map;


public class IndexUpdatePlanner implements UpdatePlanner {
    private Metadata metadata;

    public IndexUpdatePlanner(Metadata metadata) {
        this.metadata = metadata;
    }

    @Override
    public int executeInsert(InsertData data, Transaction tx) {
        TableName tableName = data.tableName();
        Plan p = new TablePlan(tx, tableName, metadata);

        // first, insert the record
        UpdateScan s = (UpdateScan) p.open();
        s.insert();
        RId rid = s.getRid();

        // then modify each field, inserting an index record if appropriate
        Map<FieldName, IndexStat> indexes = metadata.getIndexInfo(tableName, tx);
        Iterator<DataBox<?>> valIter = data.vals().iterator();
        for (FieldName fieldName : data.fields()) {
            DataBox<?> val = valIter.next();
            s.setVal(fieldName, val);

            IndexStat ii = indexes.get(fieldName);
            if (ii != null) {
                Index idx = ii.open();
                idx.insert(val, rid);
                idx.close();
            }
        }
        s.close();
        return 1;

    }

    @Override
    public int executeDelete(DeleteData data, Transaction tx) {
        TableName tblname = data.tableName();
        Plan p = new TablePlan(tx, tblname, metadata);
        p = new SelectPlan(p, data.predicate());
        Map<FieldName, IndexStat> indexes = metadata.getIndexInfo(tblname, tx);

        UpdateScan s = (UpdateScan) p.open();
        int count = 0;
        while (s.next()) {
            // first, delete the record's RID from every index
            RId rid = s.getRid();
            for (FieldName fieldName : indexes.keySet()) {
                DataBox<?> val = s.getVal(fieldName);
                Index idx = indexes.get(fieldName).open();
                idx.delete(val, rid);
                idx.close();
            }
            // then delete the record
            s.delete();
            count++;
        }
        s.close();
        return count;

    }

    @Override
    public int executeModify(ModifyData data, Transaction tx) {
        TableName tableName = data.tableName();
        FieldName fieldName = data.targetField();
        Plan p = new TablePlan(tx, tableName, metadata);
        p = new SelectPlan(p, data.predicate());

        IndexStat ii = metadata.getIndexInfo(tableName, tx).get(fieldName);
        Index idx = (ii == null) ? null : ii.open();

        UpdateScan s = (UpdateScan) p.open();
        int count = 0;
        while (s.next()) {
            // first, update the record
            DataBox<?> newVal = data.newValue().evaluate(s);
            DataBox<?> oldVal = s.getVal(fieldName);
            s.setVal(data.targetField(), newVal);

            // then update the appropriate index, if it exists
            if (idx != null) {
                RId rid = s.getRid();
                idx.delete(oldVal, rid);
                idx.insert(newVal, rid);
            }
            count++;
        }
        if (idx != null) {
            idx.close();
        }
        s.close();
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
