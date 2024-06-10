package com.mammb.code.db.index;

import com.mammb.code.db.Index;
import com.mammb.code.db.Layout;
import com.mammb.code.db.RId;
import com.mammb.code.db.Schema;
import com.mammb.code.db.Table;
import com.mammb.code.db.Transaction;
import com.mammb.code.db.lang.DataBox;
import com.mammb.code.db.lang.FieldName;
import com.mammb.code.db.lang.IdxName;
import com.mammb.code.db.lang.TableName;

public class HashIndex implements Index {
    public static int NUM_BUCKETS = 100;
    static final FieldName BLOCK = new FieldName("block");
    static final FieldName ID = new FieldName("id");
    static final FieldName DATA_VAL = new FieldName("data_val");

    private IdxName name;
    private Transaction tx;
    private Table table;
    private DataBox<?> searchKey;
    private Schema tableSchema;
    private FieldName fieldName;

    public HashIndex(Transaction tx, IdxName name, Schema tableSchema, FieldName fieldName) {
        this.tx = tx;
        this.name = name;
        this.tableSchema = tableSchema;
        this.fieldName = fieldName;
    }

    @Override
    public void beforeFirst(DataBox<?> searchKey) {
        close();
        this.searchKey = searchKey;
        int bucket = searchKey.hashCode() % NUM_BUCKETS;
        TableName tableName = TableName.of(name.val() + bucket);
        table = new Table(tx, tableName, createIdxLayout(fieldName, tableSchema));
    }

    @Override
    public boolean next() {
        while (table.next()) {
            if (table.getVal(DATA_VAL).equals(searchKey)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public RId getDataRid() {
        return new RId(table.getInt(BLOCK), table.getInt(ID));
    }

    @Override
    public void insert(DataBox<?> val, RId rid) {
        beforeFirst(val);
        table.insert();
        table.setInt(BLOCK, rid.blockNum());
        table.setInt(ID, rid.slot());
        table.setVal(DATA_VAL, val);
    }

    @Override
    public void delete(DataBox<?> val, RId rid) {
        beforeFirst(val);
        while (next()) {
            if (getDataRid().equals(rid)) {
                table.delete();
                return;
            }
        }
    }

    @Override
    public void close() {
        if (table != null) {
            table.close();
        }
    }

    private Layout createIdxLayout(FieldName fieldName, Schema tableSchema) {
        Schema schema = new Schema();
        schema.addIntField(BLOCK);
        schema.addIntField(ID);
        if (tableSchema.type(fieldName) == java.sql.Types.INTEGER) {
            schema.addIntField(DATA_VAL);
        } else {
            schema.addStringField(DATA_VAL, tableSchema.length(fieldName));
        }
        return new Layout(schema);
    }

    public static int searchCost(int blocks, int rpb) {
        return blocks / NUM_BUCKETS;
    }
}
