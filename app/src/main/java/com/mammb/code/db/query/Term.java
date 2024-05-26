package com.mammb.code.db.query;

import com.mammb.code.db.Schema;
import com.mammb.code.db.lang.DataBox;
import com.mammb.code.db.lang.FieldName;
import com.mammb.code.db.plan.Plan;

public class Term {
    private Expression lhs, rhs;

    public Term(Expression lhs, Expression rhs) {
        this.lhs = lhs;
        this.rhs = rhs;
    }

    public boolean isSatisfied(Scan s) {
        return rhs.evaluate(s).equals(lhs.evaluate(s));
    }

    public int reductionFactor(Plan p) {
        FieldName lhsName, rhsName;
        if (lhs.isFieldName() && rhs.isFieldName()) {
            lhsName = lhs.asFieldName();
            rhsName = rhs.asFieldName();
            return Math.max(p.distinctValues(lhsName),
                p.distinctValues(rhsName));
        }
        if (lhs.isFieldName()) {
            lhsName = lhs.asFieldName();
            return p.distinctValues(lhsName);
        }
        if (rhs.isFieldName()) {
            rhsName = rhs.asFieldName();
            return p.distinctValues(rhsName);
        }
        // otherwise, the term equates constants
        if (lhs.asConstant().equals(rhs.asConstant())) {
            return 1;
        } else {
            return Integer.MAX_VALUE;
        }
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

    public boolean appliesTo(Schema schema) {
        return lhs.appliesTo(schema) && rhs.appliesTo(schema);
    }

    public String toString() {
        return lhs.toString() + "=" + rhs.toString();
    }
}
