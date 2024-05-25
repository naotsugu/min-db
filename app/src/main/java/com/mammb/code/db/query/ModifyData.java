package com.mammb.code.db.query;

import com.mammb.code.db.lang.FieldName;
import com.mammb.code.db.lang.TableName;

public record ModifyData(
    TableName tableName,
    FieldName targetField,
    Expression newValue,
    Predicate predicate) {
}
