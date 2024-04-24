package com.mammb.code.db;

import java.nio.file.Path;

public class DataBase {
    private static final System.Logger logger = System.getLogger(DataBase.class.getName());
    public static final int BLOCK_SIZE = 400;
    public static final int BUFFER_SIZE = 8;

    private final DataFile dataFile;
    private final BufferPool bufferPool;

    DataBase(Path baseDirectory, int blockSize, int bufferSize) {
        dataFile = new DataFile(baseDirectory, blockSize);
        bufferPool = new BufferPool(dataFile, bufferSize);
    }

    public DataBase(Path baseDirectory) {
        this(baseDirectory, BLOCK_SIZE, BUFFER_SIZE);
        Transaction tx = newTx();
        // TODO
        tx.commit();
    }

    public Transaction newTx() {
        return new Transaction(dataFile, bufferPool);
    }


}
