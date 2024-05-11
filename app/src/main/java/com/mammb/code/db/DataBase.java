package com.mammb.code.db;

import java.nio.file.Path;

public class DataBase {
    private static final System.Logger log = System.getLogger(DataBase.class.getName());

    private final DataFile dataFile;
    private final BufferPool bufferPool;
    private final TransactionLog txLog;
    private final Metadata metadata;

    public DataBase(Path baseDirectory) {

        dataFile = new DataFile(baseDirectory);
        boolean recover = !dataFile.isEmpty();

        txLog = new TransactionLog(dataFile);
        bufferPool = new BufferPool(dataFile, txLog);

        Transaction tx = newTx();
        if (recover) {
            tx.recover();
            metadata = new Metadata();
        } else {
            log.log(System.Logger.Level.INFO, "Creating new database");
            metadata = new Metadata();
            metadata.init(tx);
        }
        tx.commit();
    }

    public Transaction newTx() {
        return new Transaction(dataFile, txLog, bufferPool);
    }

}
