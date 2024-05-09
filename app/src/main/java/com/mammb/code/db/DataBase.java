package com.mammb.code.db;

import java.nio.file.Path;

public class DataBase {
    private static final System.Logger log = System.getLogger(DataBase.class.getName());

    private final DataFile dataFile;
    private final BufferPool bufferPool;
    private final TransactionLog txLog;
    private final Metadata metadata;
    private boolean initialized;

    public DataBase(Path baseDirectory) {

        dataFile = new DataFile(baseDirectory);
        initialized = !dataFile.isEmpty();
        txLog = new TransactionLog(dataFile, "transaction.log");
        bufferPool = new BufferPool(dataFile, txLog);
        metadata = new Metadata();
        Transaction tx = newTx();
        if (initialized) {
            tx.recover();
        } else {
            metadata.init(tx);
            initialized = true;
        }
        tx.commit();
    }

    public Transaction newTx() {
        return new Transaction(dataFile, txLog, bufferPool);
    }

}
