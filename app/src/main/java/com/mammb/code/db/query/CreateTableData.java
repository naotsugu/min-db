package com.mammb.code.db.query;

import com.mammb.code.db.Schema;
import com.mammb.code.db.lang.TableName;

public record CreateTableData(Schema schema) {
    public TableName tableName() {
        return schema.tableName();
    }
}
