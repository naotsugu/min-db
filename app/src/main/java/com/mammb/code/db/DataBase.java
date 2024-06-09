package com.mammb.code.db;

import com.mammb.code.db.plan.Planner;
import java.nio.file.Path;

public class DataBase {
    private static final System.Logger log = System.getLogger(DataBase.class.getName());

    private final DataFile dataFile;
    private final BlockPool blockPool;
    private final TransactionLog txLog;
    private final Metadata metadata;
    private final Planner planner;

    public DataBase(Path baseDirectory) {

        dataFile = new DataFile(baseDirectory);
        boolean recover = !dataFile.isEmpty();

        txLog = new TransactionLog(dataFile);
        blockPool = new BlockPool(dataFile, txLog);

        Transaction tx = newTx();
        if (recover) {
            log.log(System.Logger.Level.INFO, "recovering existing database");
            tx.recover();
            metadata = new Metadata();
        } else {
            log.log(System.Logger.Level.INFO, "Creating new database");
            metadata = new Metadata();
            metadata.init(tx);
        }
        planner = new Planner(metadata);
        tx.commit();
    }

    public Transaction newTx() {
        return new Transaction(dataFile, txLog, blockPool);
    }

    public Planner planner() {
        return planner;
    }
}
