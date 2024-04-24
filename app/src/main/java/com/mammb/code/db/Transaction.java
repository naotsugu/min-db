package com.mammb.code.db;

public class Transaction {
    private final DataFile dataFile;
    private final BufferPool bufferPool;
    private BufferList bufferList;

    private static int nextTxNum = 0;

    public Transaction(DataFile dataFile, BufferPool bufferPool) {
        this.dataFile = dataFile;
        this.bufferPool = bufferPool;
        this.bufferList = new BufferList(bufferPool);
    }

    public void pin(BlockId blockId) {
        bufferList.pin(blockId);
    }

    public void unpin(BlockId blockId) {
        bufferList.unpin(blockId);
    }

    public int blockSize() {
        return dataFile.blockSize();
    }

    public int availableBuffs() {
        return bufferPool.availableCount();
    }

    private static synchronized int nextTxNumber() {
        return ++nextTxNum;
    }

}
