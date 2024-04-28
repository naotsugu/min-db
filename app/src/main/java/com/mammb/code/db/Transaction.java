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

    public void recover() {
        bufferPool.flushAll(txn);
        //recoveryMgr.recover();
    }

    public void pin(BlockId blockId) {
        bufferList.pin(blockId);
    }

    public void unpin(BlockId blockId) {
        bufferList.unpin(blockId);
    }

    public int getInt(BlockId blockId, int offset) {
        //concurMgr.sLock(blk);
        Buffer buff = bufferList.getBuffer(blockId);
        return buff.contents().getInt(offset);
    }

    public String getString(BlockId blockId, int offset) {
        //concurMgr.sLock(blk);
        Buffer buff = bufferList.getBuffer(blockId);
        return buff.contents().getString(offset);
    }

    public void setInt(BlockId blockId, int offset, int val, boolean okToLog) {
        //concurMgr.xLock(blk);
        Buffer buff = bufferList.getBuffer(blockId);
        int lsn = -1;
        if (okToLog) {
            //lsn = recoveryMgr.setInt(buff, offset, val);
        }
        Page p = buff.contents();
        p.setInt(offset, val);
        buff.setModified(txn, lsn);
    }

    public void setString(BlockId blockId, int offset, String val, boolean okToLog) {
        //concurMgr.xLock(blk);
        Buffer buff = bufferList.getBuffer(blockId);
        int lsn = -1;
        if (okToLog) {
            //lsn = recoveryMgr.setString(buff, offset, val);
        }
        Page p = buff.contents();
        p.setString(offset, val);
        buff.setModified(txn, lsn);
    }

    public long size(BlockId blockId) {
        //concurMgr.sLock(blockId);
        return dataFile.length(blockId.fileName());
    }

    public void append(BlockId blockId) {
        //concurMgr.xLock(blockId);
        dataFile.append(blockId.fileName());
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
