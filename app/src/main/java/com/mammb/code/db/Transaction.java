package com.mammb.code.db;

public class Transaction {

    private static final System.Logger log = System.getLogger(Transaction.class.getName());

    private static int nextTxNum = 0;

    private final DataFile dataFile;
    private final BufferPool bufferPool;
    private BufferList bufferList;

    private int txn;

    public Transaction(DataFile dataFile, BufferPool bufferPool) {
        this.dataFile = dataFile;
        this.bufferPool = bufferPool;
        this.bufferList = new BufferList(bufferPool);
    }

    public void commit() {
        //recoveryMgr.commit();
        log.log(System.Logger.Level.INFO, "transaction " + txn + " committed");
        //concurMgr.release();
        bufferList.unpinAll();
    }

    public void rollback() {
        //recoveryMgr.rollback();
        log.log(System.Logger.Level.INFO, "transaction " + txn + " rolled back");
        //concurMgr.release();
        bufferList.unpinAll();
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
