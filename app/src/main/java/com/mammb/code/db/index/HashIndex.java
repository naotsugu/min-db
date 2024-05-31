package com.mammb.code.db.index;

import com.mammb.code.db.Layout;
import com.mammb.code.db.RId;
import com.mammb.code.db.Table;
import com.mammb.code.db.Transaction;
import com.mammb.code.db.lang.DataBox;
import com.mammb.code.db.lang.FieldName;
import com.mammb.code.db.lang.IdxName;
import com.mammb.code.db.lang.TableName;

public class HashIndex implements Index {
    static final FieldName BLOCK = new FieldName("block");
    static final FieldName ID = new FieldName("id");
    static final FieldName VAL = new FieldName("data_val");

    public static int NUM_BUCKETS = 100;
    private Transaction tx;
    private IdxName idxName;
    private Layout layout;
    private DataBox<?> searchKey = null;
    private Table ts = null;

    public HashIndex(Transaction tx, IdxName idxName, Layout layout) {
        this.tx = tx;
        this.idxName = idxName;
        this.layout = layout;
    }

    public void beforeFirst(DataBox<?> searchKey) {
        close();
        this.searchKey = searchKey;
        int bucket = searchKey.hashCode() % NUM_BUCKETS;
        TableName tableName = TableName.of(idxName.val() + bucket);
        ts = new Table(tx, tableName, layout);
    }

    public boolean next() {
        while (ts.next()) {
            if (ts.getVal(VAL).equals(searchKey)) {
                return true;
            }
        }
        return false;
    }

    public RId getRid() {
        int blockNum = ts.getInt(BLOCK);
        int id = ts.getInt(ID);
        return new RId(blockNum, id);
    }

    public void insert(DataBox<?> val, RId rid) {
        beforeFirst(val);
        ts.insert();
        ts.setInt(BLOCK, rid.blockNum());
        ts.setInt(ID, rid.slot());
        ts.setVal(VAL, val);
    }

    public void delete(DataBox<?> val, RId rid) {
        beforeFirst(val);
        while (next()) {
            if (getRid().equals(rid)) {
                ts.delete();
                return;
            }
        }
    }

    public void close() {
        if (ts != null) {
            ts.close();
        }
    }

    public static int searchCost(int numBlocks, int rpb) {
        return numBlocks / HashIndex.NUM_BUCKETS;
    }

}
