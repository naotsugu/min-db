package com.mammb.code.db.plan;

import com.mammb.code.db.Transaction;
import com.mammb.code.db.query.QueryData;

public interface QueryPlanner {
    Plan createPlan(QueryData data, Transaction tx);
}
