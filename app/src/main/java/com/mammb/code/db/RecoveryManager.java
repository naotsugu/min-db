package com.mammb.code.db;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RecoveryManager {
    private int txn;
    private final Transaction tx;
    private final TransactionLog txLog;
    private final BufferPool bufferPool;

    public RecoveryManager(Transaction tx, int txn, TransactionLog txLog, BufferPool bufferPool) {
        this.tx = tx;
        this.txn = txn;
        this.txLog = txLog;
        this.bufferPool = bufferPool;
        new LogRecord.Start(txn).write(txLog);
    }

    public void commit() {
        bufferPool.flushAll(txn);
        int lsn = new LogRecord.Commit(txn).write(txLog);
        txLog.flush(lsn);
    }

    public void rollback() {
        doRollback();
        bufferPool.flushAll(txn);
        int lsn = new LogRecord.Rollback(txn).write(txLog);
        txLog.flush(lsn);
    }

    public void recover() {
        doRecover();
        bufferPool.flushAll(txn);
        int lsn = new LogRecord.CheckPoint().write(txLog);
        txLog.flush(lsn);
    }

    public int setInt(Block buff, int offset, int newVal) {
        int oldVal = buff.contents().getInt(offset);
        BlockId blockId = buff.blockId();
        return new LogRecord.SetInt(txn, offset, oldVal, blockId).write(txLog);
    }

    public int setString(Block buff, int offset, String newVal) {
        String oldVal = buff.contents().getString(offset);
        BlockId blockId = buff.blockId();
        return new LogRecord.SetString(txn, offset, oldVal, blockId).write(txLog);
    }

    private void doRollback() {
        txLog.iterator().forEachRemaining(bytes -> {
            switch (LogRecord.createLogRecord(bytes)) {
                case LogRecord.SetInt r -> { if (r.txn() == txn) r.undo(tx); }
                case LogRecord.SetString r -> { if (r.txn() == txn) r.undo(tx); }
                default -> { }
            }
        });
    }

    private void doRecover() {
        List<Integer> finishedTxs = new ArrayList<>();
        Iterator<byte[]> iterator = txLog.iterator();
        while (iterator.hasNext()) {
            byte[] bytes = iterator.next();
            LogRecord rec = LogRecord.createLogRecord(bytes);
            switch (rec) {
                case LogRecord.CheckPoint checkPoint -> {
                    return;
                }
                case LogRecord.Commit commit -> finishedTxs.add(commit.txn());
                case LogRecord.Rollback rollback -> finishedTxs.add(rollback.txn());
                case LogRecord.Txn txn -> {
                    if (!finishedTxs.contains(txn.txn())) {
                        rec.undo(tx);
                    }
                }
                default -> {
                }
            }
        }
    }
}
