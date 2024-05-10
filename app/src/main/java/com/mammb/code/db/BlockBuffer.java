package com.mammb.code.db;

public class BlockBuffer {
    private DataFile dataFile;
    private TransactionLog transactionLog;
    private ByteBuffer contents;
    private BlockId blockId;
    private int pins = 0;
    private int txn = -1;
    private int lsn = -1;

    public BlockBuffer(DataFile dataFile, TransactionLog transactionLog) {
        this.dataFile = dataFile;
        this.transactionLog = transactionLog;
        contents = new ByteBuffer(dataFile.blockSize());
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
            transactionLog.flush(lsn);
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
