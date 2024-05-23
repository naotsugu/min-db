package com.mammb.code.db.query;

import com.mammb.code.db.lang.DataBox;
import com.mammb.code.db.lang.FieldName;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Predicate {
    private List<Term> terms = new ArrayList<>();

    public Predicate() {
    }

    public Predicate(Term t) {
        terms.add(t);
    }

    public void conjoinWith(Predicate predicate) {
        terms.addAll(predicate.terms);
    }


    public DataBox<?> equatesWithConstant(FieldName fieldName) {
        for (Term t : terms) {
            DataBox<?> c = t.equatesWithConstant(fieldName);
            if (c != null) {
                return c;
            }
        }
        return null;
    }

    public FieldName equatesWithField(FieldName fieldName) {
        for (Term term : terms) {
            FieldName s = term.equatesWithField(fieldName);
            if (s != null) {
                return s;
            }
        }
        return null;
    }

    public String toString() {
        Iterator<Term> itr = terms.iterator();
        if (!itr.hasNext()) {
            return "";
        }
        StringBuilder sb = new StringBuilder(itr.next().toString());
        while (itr.hasNext()) {
            sb.append(" and ");
            sb.append(itr.next().toString());
        }
        return sb.toString();
    }
}
