package com.mammb.code.db;

import java.util.ArrayList;
import java.util.List;

public class RecoveryManager {
    private int txn;
    private Transaction tx;
    private TransactionLog txLog;
    private BufferPool bufferPool;

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

    public int setInt(Buffer buff, int offset, int newVal) {
        int oldVal = buff.contents().getInt(offset);
        BlockId blockId = buff.blockId();
        return new LogRecord.SetInt(txn, offset, oldVal, blockId).write(txLog);
    }

    public int setString(Buffer buff, int offset, String newVal) {
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
        txLog.iterator().forEachRemaining(bytes -> {
            switch (LogRecord.createLogRecord(bytes)) {
                case LogRecord.CheckPoint r -> { }
                case LogRecord.Commit r -> finishedTxs.add(r.txn());
                case LogRecord.Rollback r -> finishedTxs.add(r.txn());
                case LogRecord.Start r -> { if (!finishedTxs.contains(r.txn())) r.undo(tx); }
                case LogRecord.SetInt r -> { if (!finishedTxs.contains(r.txn())) r.undo(tx); }
                case LogRecord.SetString r -> { if (!finishedTxs.contains(r.txn())) r.undo(tx); }
            }
        });
    }
}
