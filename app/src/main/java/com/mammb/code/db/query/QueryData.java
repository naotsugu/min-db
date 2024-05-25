package com.mammb.code.db.query;

import com.mammb.code.db.lang.FieldName;
import com.mammb.code.db.lang.TableName;
import java.util.Collection;
import java.util.List;

public class QueryData {
    private List<FieldName> fields;
    private Collection<TableName> tables;
    private Predicate predicate;

    public QueryData(List<FieldName> fields, Collection<TableName> tables, Predicate predicate) {
        this.fields = fields;
        this.tables = tables;
        this.predicate = predicate;
    }

    public List<FieldName> fields() {
        return fields;
    }

    public Collection<TableName> tables() {
        return tables;
    }

    public Predicate pred() {
        return predicate;
    }

    public String toString() {
        String result = "select ";
        for (FieldName filename : fields) {
            result += filename.val() + ", ";
        }
        result = result.substring(0, result.length() - 2); //remove final comma
        result += " from ";
        for (TableName tableName : tables) {
            result += tableName.val() + ", ";
        }
        result = result.substring(0, result.length() - 2); //remove final comma
        String predstring = predicate.toString();
        if (!predstring.isEmpty()) {
            result += " where " + predstring;
        }
        return result;
    }

}
