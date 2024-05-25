package com.mammb.code.db.query;

import com.mammb.code.db.lang.FieldName;
import com.mammb.code.db.lang.IdxName;
import com.mammb.code.db.lang.TableName;

public record CreateIndexData(
    IdxName indexName,
    TableName tableName,
    FieldName fieldName) {
}
