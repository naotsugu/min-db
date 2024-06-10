package com.mammb.code.db.query;

import com.mammb.code.db.Schema;
import com.mammb.code.db.lang.DataBox;
import com.mammb.code.db.lang.FieldName;
import com.mammb.code.db.lang.TableName;
import com.mammb.code.db.plan.Plan;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Predicate {
    private final List<Term> terms = new ArrayList<>();

    public Predicate() {
    }

    public Predicate(Term t) {
        terms.add(t);
    }

    public void conjoinWith(Predicate predicate) {
        terms.addAll(predicate.terms);
    }

    public boolean isSatisfied(Scan s) {
        for (Term t : terms) {
            if (!t.isSatisfied(s)) {
                return false;
            }
        }
        return true;
    }

    public int reductionFactor(Plan p) {
        int factor = 1;
        for (Term t : terms) {
            factor *= t.reductionFactor(p);
        }
        return factor;
    }

    public Predicate selectSubPred(Schema sch) {
        Predicate result = new Predicate();
        for (Term t : terms) {
            if (t.appliesTo(sch))
                result.terms.add(t);
        }
        if (result.terms.isEmpty()) {
            return null;
        } else {
            return result;
        }
    }

    public Predicate joinSubPred(Schema sch1, Schema sch2) {
        Predicate result = new Predicate();
        Schema newsch = new Schema();
        newsch.addAll(sch1);
        newsch.addAll(sch2);
        for (Term t : terms) {
            if (!t.appliesTo(sch1) && !t.appliesTo(sch2) && t.appliesTo(newsch)) {
                result.terms.add(t);
            }
        }
        if (result.terms.isEmpty()) {
            return null;
        } else {
            return result;
        }
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
