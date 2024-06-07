package com.mammb.code.db.plan;

import com.mammb.code.db.Metadata;
import com.mammb.code.db.Transaction;
import com.mammb.code.db.query.*;
import com.mammb.code.db.query.Parser.CreateIndexData;
import com.mammb.code.db.query.Parser.CreateTableData;
import com.mammb.code.db.query.Parser.DeleteData;
import com.mammb.code.db.query.Parser.InsertData;
import com.mammb.code.db.query.Parser.ModifyData;

public class Planner {
    private final QueryPlanner queryPlanner;
    private final UpdatePlanner updatePlanner;

    public Planner(QueryPlanner queryPlanner, UpdatePlanner updatePlanner) {
        this.queryPlanner = queryPlanner;
        this.updatePlanner = updatePlanner;
    }

    public Planner(Metadata metadata) {
        this(new BasicQueryPlanner(metadata), new BasicUpdatePlanner(metadata));
    }

    public Plan createQueryPlan(String qry, Transaction tx) {
        Parser parser = Parser.of(qry);
        Parser.QueryData data = parser.query();
        verifyQuery(data);
        return queryPlanner.createPlan(data, tx);
    }

    public int executeUpdate(String cmd, Transaction tx) {
        Parser parser = Parser.of(cmd);
        Object data = parser.updateCmd();
        verifyUpdate(data);
        return switch (data) {
            case InsertData insertData -> updatePlanner.executeInsert(insertData, tx);
            case DeleteData deleteData -> updatePlanner.executeDelete(deleteData, tx);
            case ModifyData modifyData -> updatePlanner.executeModify(modifyData, tx);
            case CreateTableData createTable -> updatePlanner.executeCreateTable(createTable, tx);
            case CreateIndexData createIndex -> updatePlanner.executeCreateIndex(createIndex, tx);
            default -> 0;
        };
    }

    private void verifyQuery(Parser.QueryData data) {
    }

    private void verifyUpdate(Object data) {
    }

}
