package com.mammb.code.db.plan;

import com.mammb.code.db.Metadata;
import com.mammb.code.db.Transaction;
import com.mammb.code.db.query.Parser.QueryData;
import java.util.List;
import java.util.stream.Collectors;

public class BasicQueryPlanner implements QueryPlanner {
    private final Metadata metadata;

    public BasicQueryPlanner(Metadata metadata) {
        this.metadata = metadata;
    }

    @Override
    public Plan createPlan(QueryData data, Transaction tx) {
        //Step 1: Create a plan for each mentioned table or view.
        List<Plan> plans = data.tables().stream()
            .map(tableName -> new TablePlan(tx, tableName, metadata))
            .collect(Collectors.toList());

        //Step 2: Create the product of all table plans
        Plan p = plans.removeFirst();
        for (Plan nextplan : plans) {
            p = new ProductPlan(p, nextplan);
        }
        //Step 3: Add a selection plan for the predicate
        p = new SelectPlan(p, data.predicate());

        //Step 4: Project on the field names
        p = new ProjectPlan(p, data.fields());
        return p;
    }
}
