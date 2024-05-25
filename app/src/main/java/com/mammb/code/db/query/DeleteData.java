package com.mammb.code.db.query;

import com.mammb.code.db.lang.TableName;

public record DeleteData(
    TableName tableName,
    Predicate predicate) {
}
