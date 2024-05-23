package com.mammb.code.db.query;

import com.mammb.code.db.lang.DataBox;
import com.mammb.code.db.lang.FieldName;

public class Term {
    private Expression lhs, rhs;

    public Term(Expression lhs, Expression rhs) {
        this.lhs = lhs;
        this.rhs = rhs;
    }

    public DataBox<?> equatesWithConstant(FieldName fieldName) {
        if (lhs.isFieldName() &&
            lhs.asFieldName().equals(fieldName) &&
            !rhs.isFieldName())
            return rhs.asConstant();
        else if (rhs.isFieldName() &&
            rhs.asFieldName().equals(fieldName) &&
            !lhs.isFieldName())
            return lhs.asConstant();
        else
            return null;
    }

    public FieldName equatesWithField(FieldName fieldName) {
        if (lhs.isFieldName() &&
            lhs.asFieldName().equals(fieldName) &&
            rhs.isFieldName())
            return rhs.asFieldName();
        else if (rhs.isFieldName() &&
            rhs.asFieldName().equals(fieldName) &&
            lhs.isFieldName())
            return lhs.asFieldName();
        else
            return null;
    }

    public String toString() {
        return lhs.toString() + "=" + rhs.toString();
    }
}
