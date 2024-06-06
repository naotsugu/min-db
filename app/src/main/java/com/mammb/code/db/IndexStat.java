package com.mammb.code.db;

import com.mammb.code.db.index.HashIndex;
import com.mammb.code.db.lang.FieldName;
import com.mammb.code.db.lang.IdxName;

public class IndexStat {
    private final IdxName name;
    private final FieldName fieldName;
    private final Layout layout;
    private final Schema tableSchema;
    private final Statistics.Stat stat;
    private final Transaction tx;

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
        return new HashIndex(tx, name, tableSchema, fieldName);
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
