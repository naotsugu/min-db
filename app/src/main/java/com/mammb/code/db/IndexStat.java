package com.mammb.code.db;

import com.mammb.code.db.lang.FieldName;
import com.mammb.code.db.lang.IdxName;
import simpledb.index.hash.HashIndex;

public class IndexStat {
    private IdxName name;
    private FieldName fieldName;
    private Layout layout;
    private Schema tableSchema;
    private Statistics.Stat stat;
    private Transaction tx;

    public IndexStat(IdxName name, FieldName fieldName, Schema tableSchema,
            Transaction tx, Layout layout, Statistics.Stat stat) {
        this.name = name;
        this.fieldName = fieldName;
        this.tableSchema = tableSchema;
        this.layout = layout;
        this.stat = stat;
        this.tx = tx;
    }

    public Index open() {
        return Index.hashIndex(tx, name, tableSchema, fieldName);
    }

    public int blocksAccessed() {
        int rpb = tx.blockSize() / layout.slotSize();
        int blocks = stat.numRecs() / rpb;
        return HashIndex.searchCost(blocks, rpb);
    }

    public int recordsOutput() {
        return stat.numRecs() / stat.distinctValues(fieldName);
    }

    public int distinctValues(FieldName fn) {
        return fieldName.equals(fn) ? 1 : stat.distinctValues(fieldName);
    }

}
