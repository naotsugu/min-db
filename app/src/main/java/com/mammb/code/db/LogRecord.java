package com.mammb.code.db;

import com.mammb.code.db.lang.ByteBuffer;

public sealed interface LogRecord {

    interface Txn { int txn(); }

    record CheckPoint() implements LogRecord {}
    record Start(int txn) implements LogRecord, Txn {}
    record Commit(int txn) implements LogRecord, Txn {}
    record Rollback(int txn) implements LogRecord, Txn {}
    record SetInt(int txn, int offset, int val, BlockId blockId) implements LogRecord, Txn {}
    record SetString(int txn, int offset, String val, BlockId blockId) implements LogRecord, Txn {}


    static LogRecord createLogRecord(byte[] bytes) {
        ByteBuffer byteBuffer = new ByteBuffer(bytes);
        return switch (byteBuffer.getInt(0)) {
            case 0 -> new CheckPoint();
            case 1 -> new Start(byteBuffer.getInt(Integer.BYTES));
            case 2 -> new Commit(byteBuffer.getInt(Integer.BYTES));
            case 3 -> new Rollback(byteBuffer.getInt(Integer.BYTES));
            case 4 -> {
                Pos pos = Pos.of(byteBuffer);
                yield new SetInt(
                    byteBuffer.getInt(pos.txn),
                    byteBuffer.getInt(pos.offset),
                    byteBuffer.getInt(pos.value),
                    new BlockId(byteBuffer.getString(pos.file), byteBuffer.getInt(pos.block)));
            }
            case 5 -> {
                Pos pos = Pos.of(byteBuffer);
                yield new SetString(
                    byteBuffer.getInt(pos.txn),
                    byteBuffer.getInt(pos.offset),
                    byteBuffer.getString(pos.value),
                    new BlockId(byteBuffer.getString(pos.file), byteBuffer.getInt(pos.block)));
            }
            default -> throw new RuntimeException();
        };
    }

    default void undo(Transaction tx) {
        switch (this) {
            case SetInt r -> {
                tx.pin(r.blockId);
                tx.setInt(r.blockId, r.offset, r.val, false);
                tx.unpin(r.blockId);
            }
            case SetString r -> {
                tx.pin(r.blockId);
                tx.setString(r.blockId, r.offset, r.val, false);
                tx.unpin(r.blockId);
            }
            default -> { }
        };
    }

    default int write(TransactionLog transactionLog) {
        return switch (this) {
            case CheckPoint r -> {
                byte[] rec = new byte[Integer.BYTES];
                ByteBuffer byteBuffer = new ByteBuffer(rec);
                byteBuffer.setInt(0, 0);
                yield transactionLog.append(rec);
            }
            case Start r -> {
                byte[] rec = new byte[2 * Integer.BYTES];
                ByteBuffer byteBuffer = new ByteBuffer(rec);
                byteBuffer.setInt(0, 1);
                byteBuffer.setInt(Integer.BYTES, r.txn);
                yield transactionLog.append(rec);
            }
            case Commit r -> {
                byte[] rec = new byte[2 * Integer.BYTES];
                ByteBuffer byteBuffer = new ByteBuffer(rec);
                byteBuffer.setInt(0, 2);
                byteBuffer.setInt(Integer.BYTES, r.txn);
                yield transactionLog.append(rec);
            }
            case Rollback r -> {
                byte[] rec = new byte[2 * Integer.BYTES];
                ByteBuffer byteBuffer = new ByteBuffer(rec);
                byteBuffer.setInt(0, 3);
                byteBuffer.setInt(Integer.BYTES, r.txn);
                yield transactionLog.append(rec);
            }
            case SetInt r -> {
                Pos pos = Pos.of(r.blockId);
                byte[] rec = new byte[pos.value + Integer.BYTES];
                ByteBuffer byteBuffer = new ByteBuffer(rec);
                byteBuffer.setInt(0, 4);
                byteBuffer.setInt(pos.txn, r.txn);
                byteBuffer.setString(pos.file, r.blockId.fileName());
                byteBuffer.setInt(pos.block, r.blockId.number());
                byteBuffer.setInt(pos.offset, r.offset);
                byteBuffer.setInt(pos.value, r.val);
                yield transactionLog.append(rec);
            }
            case SetString r -> {
                Pos pos = Pos.of(r.blockId);
                byte[] rec = new byte[pos.value + ByteBuffer.maxLength(r.val.length())];
                ByteBuffer byteBuffer = new ByteBuffer(rec);
                byteBuffer.setInt(0, 5);
                byteBuffer.setInt(pos.txn, r.txn);
                byteBuffer.setString(pos.file, r.blockId.fileName());
                byteBuffer.setInt(pos.block, r.blockId.number());
                byteBuffer.setInt(pos.offset, r.offset);
                byteBuffer.setString(pos.value, r.val);
                yield transactionLog.append(rec);
            }
        };
    }

    record Pos(int txn, int file, int block, int offset, int value) {
        static Pos of(ByteBuffer page) {
            int txn = Integer.BYTES;
            int file = txn + Integer.BYTES;
            int block = file + ByteBuffer.maxLength(page.getString(file).length());
            int offset = block + Integer.BYTES;
            int value = offset + Integer.BYTES;
            return new Pos(txn, file, block, offset, value);
        }
        static Pos of(BlockId blockId) {
            int txn = Integer.BYTES;
            int file = txn + Integer.BYTES;
            int block = file + ByteBuffer.maxLength(blockId.fileName().length());
            int offset = block + Integer.BYTES;
            int value = offset + Integer.BYTES;
            return new Pos(txn, file, block, offset, value);
        }
    }

}
