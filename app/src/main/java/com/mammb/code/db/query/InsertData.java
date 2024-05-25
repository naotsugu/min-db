package com.mammb.code.db.query;

import com.mammb.code.db.lang.DataBox;
import com.mammb.code.db.lang.FieldName;
import com.mammb.code.db.lang.TableName;
import java.util.List;

public record InsertData(
    TableName tableName,
    List<FieldName> fields,
    List<DataBox<?>> vals) {
}
