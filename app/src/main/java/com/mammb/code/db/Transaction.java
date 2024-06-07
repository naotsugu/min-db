package com.mammb.code.db;

import com.mammb.code.db.lang.ByteBuffer;

public class Transaction {

    private static int nextTxNum = 0;

    private final DataFile dataFile;
    private final BufferPool bufferPool;
    private final BufferList bufferList;
    private final Lock lock;
    private final RecoveryManager rman;
    private final int txn;

    public Transaction(DataFile dataFile, TransactionLog txLog, BufferPool bufferPool) {
        this.dataFile = dataFile;
        this.bufferPool = bufferPool;
        this.txn = nextTxNumber();
        this.rman = new RecoveryManager(this, txn, txLog, bufferPool);
        this.lock = new Lock();
        this.bufferList = new BufferList(bufferPool);
    }

    public void commit() {
        rman.commit();
        System.out.println("transaction " + txn + " committed");
        lock.release();
        bufferList.unpinAll();
    }

    public void rollback() {
        rman.rollback();
        System.out.println("transaction " + txn + " rolled back");
        lock.release();
        bufferList.unpinAll();
    }

    public void recover() {
        bufferPool.flushAll(txn);
        rman.recover();
    }

    public void pin(BlockId blockId) {
        bufferList.pin(blockId);
    }

    public void unpin(BlockId blockId) {
        bufferList.unpin(blockId);
    }

    public int getInt(BlockId blockId, int offset) {
        lock.sLock(blockId);
        Block buff = bufferList.getBuffer(blockId);
        return buff.contents().getInt(offset);
    }

    public String getString(BlockId blockId, int offset) {
        lock.sLock(blockId);
        Block buff = bufferList.getBuffer(blockId);
        return buff.contents().getString(offset);
    }

    public void setInt(BlockId blockId, int offset, int val, boolean okToLog) {
        lock.xLock(blockId);
        Block buff = bufferList.getBuffer(blockId);
        int lsn = -1;
        if (okToLog) {
            lsn = rman.setInt(buff, offset, val);
        }
        ByteBuffer byteBuffer = buff.contents();
        byteBuffer.setInt(offset, val);
        buff.setModified(txn, lsn);
    }

    public void setString(BlockId blockId, int offset, String val, boolean okToLog) {
        lock.xLock(blockId);
        Block buff = bufferList.getBuffer(blockId);
        int lsn = -1;
        if (okToLog) {
            lsn = rman.setString(buff, offset, val);
        }
        ByteBuffer byteBuffer = buff.contents();
        byteBuffer.setString(offset, val);
        buff.setModified(txn, lsn);
    }

    public long size(BlockId blockId) {
        lock.sLock(blockId);
        return dataFile.length(blockId.fileName());
    }

    public BlockId append(String fileName) {
        lock.xLock(BlockId.tailOf(fileName));
        return dataFile.append(fileName);
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
