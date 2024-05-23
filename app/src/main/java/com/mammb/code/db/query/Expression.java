package com.mammb.code.db.query;

import com.mammb.code.db.Schema;
import com.mammb.code.db.lang.DataBox;
import com.mammb.code.db.lang.FieldName;

public class Expression {
    private DataBox<?> val;
    private FieldName fieldName;

    public Expression(DataBox<?> val) {
        this.val = val;
    }

    public Expression(FieldName fieldName) {
        this.fieldName = fieldName;
    }

//    public DataBox<?> evaluate(Scan s) {
//        return (val != null) ? val : s.getVal(fldname);
//    }

    public boolean isFieldName() {
        return fieldName != null;
    }

    public DataBox<?> asConstant() {
        return val;
    }

    public FieldName asFieldName() {
        return fieldName;
    }

    public boolean appliesTo(Schema schema) {
        return (val != null) ? true : schema.hasField(fieldName);
    }

    public String toString() {
        return (val != null) ? val.toString() : fieldName.val();
    }

}
