package com.mammb.code.db.plan;

import com.mammb.code.db.Metadata;
import com.mammb.code.db.Transaction;
import com.mammb.code.db.lang.TableName;
import com.mammb.code.db.query.Parser.QueryData;
import java.util.ArrayList;
import java.util.List;

public class BetterQueryPlanner implements QueryPlanner {
    private Metadata metadata;

    public BetterQueryPlanner(Metadata metadata) {
        this.metadata = metadata;
    }

    @Override
    public Plan createPlan(QueryData data, Transaction tx) {
        //Step 1: Create a plan for each mentioned table.
        List<Plan> plans = new ArrayList<Plan>();
        for (TableName tableName : data.tables()) {
            plans.add(new TablePlan(tx, tableName, metadata));
        }

        //Step 2: Create the product of all table plans
        Plan p = plans.remove(0);
        for (Plan nextplan : plans) {
            // Try both orderings and choose the one having lowest cost
            Plan choice1 = new ProductPlan(nextplan, p);
            Plan choice2 = new ProductPlan(p, nextplan);
            if (choice1.blocksAccessed() < choice2.blocksAccessed()) {
                p = choice1;
            } else {
                p = choice2;
            }
        }

        //Step 3: Add a selection plan for the predicate
        p = new SelectPlan(p, data.predicate());

        //Step 4: Project on the field names
        p = new ProjectPlan(p, data.fields());
        return p;
    }

}
