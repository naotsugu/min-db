package com.mammb.code.db;

import com.mammb.code.db.lang.ByteBuffer;

public class Block {
    private final DataFile dataFile;
    private final TransactionLog txLog;
    private final ByteBuffer contents;
    private BlockId blockId;
    private int pins = 0;
    private int txn = -1;
    private int lsn = -1;

    public Block(DataFile dataFile, TransactionLog txLog) {
        this.dataFile = dataFile;
        this.txLog = txLog;
        this.contents = new ByteBuffer(dataFile.blockSize());
    }

    public ByteBuffer contents() {
        return contents;
    }

    public BlockId blockId() {
        return blockId;
    }

    public void setModified(int txn, int lsn) {
        this.txn = txn;
        if (lsn >= 0) {
            this.lsn = lsn;
        }
    }

    public boolean isPinned() {
        return pins > 0;
    }

    public int modifyingTx() {
        return txn;
    }

    void assignToBlock(BlockId blockId) {
        flush();
        this.blockId = blockId;
        dataFile.read(this.blockId, contents);
        pins = 0;
    }

    void flush() {
        if (txn >= 0) {
            txLog.flush(lsn);
            dataFile.write(blockId, contents);
            txn = -1;
        }
    }

    void pin() {
        pins++;
    }

    void unpin() {
        pins--;
    }

}
