package com.mammb.code.db;

public sealed interface LogRecord {

    record CheckPoint() implements LogRecord {}
    record Start(int txn) implements LogRecord {}
    record Commit(int txn) implements LogRecord {}
    record Rollback(int txn) implements LogRecord {}
    record SetInt(int txn, int offset, int val, BlockId blockId) implements LogRecord {}
    record SetString(int txn, int offset, String val, BlockId blockId) implements LogRecord {}


    static LogRecord createLogRecord(byte[] bytes) {
        Page page = new Page(bytes);
        return switch (page.getInt(0)) {
            case 0 -> new CheckPoint();
            case 1 -> new Start(page.getInt(Integer.BYTES));
            case 2 -> new Commit(page.getInt(Integer.BYTES));
            case 3 -> new Rollback(page.getInt(Integer.BYTES));
            case 4 -> {
                Pos pos = Pos.of(page);
                yield new SetInt(
                    page.getInt(pos.txn),
                    page.getInt(pos.offset),
                    page.getInt(pos.value),
                    new BlockId(page.getString(pos.file), page.getInt(pos.block)));
            }
            case 5 -> {
                Pos pos = Pos.of(page);
                yield new SetString(
                    page.getInt(pos.txn),
                    page.getInt(pos.offset),
                    page.getString(pos.value),
                    new BlockId(page.getString(pos.file), page.getInt(pos.block)));
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
                Page page = new Page(rec);
                page.setInt(0, 0);
                yield transactionLog.append(rec);
            }
            case Start r -> {
                byte[] rec = new byte[2 * Integer.BYTES];
                Page p = new Page(rec);
                p.setInt(0, 1);
                p.setInt(Integer.BYTES, r.txn);
                yield transactionLog.append(rec);
            }
            case Commit r -> {
                byte[] rec = new byte[2 * Integer.BYTES];
                Page p = new Page(rec);
                p.setInt(0, 2);
                p.setInt(Integer.BYTES, r.txn);
                yield transactionLog.append(rec);
            }
            case Rollback r -> {
                byte[] rec = new byte[2 * Integer.BYTES];
                Page p = new Page(rec);
                p.setInt(0, 3);
                p.setInt(Integer.BYTES, r.txn);
                yield transactionLog.append(rec);
            }
            case SetInt r -> {
                Pos pos = Pos.of(r.blockId);
                byte[] rec = new byte[pos.value + Integer.BYTES];
                Page p = new Page(rec);
                p.setInt(0, 4);
                p.setInt(pos.txn, r.txn);
                p.setString(pos.file, r.blockId.fileName());
                p.setInt(pos.block, r.blockId.number());
                p.setInt(pos.offset, r.offset);
                p.setInt(pos.value, r.val);
                yield transactionLog.append(rec);
            }
            case SetString r -> {
                Pos pos = Pos.of(r.blockId);
                byte[] rec = new byte[pos.value + Page.maxLength(r.val.length())];
                Page p = new Page(rec);
                p.setInt(0, 5);
                p.setInt(pos.txn, r.txn);
                p.setString(pos.file, r.blockId.fileName());
                p.setInt(pos.block, r.blockId.number());
                p.setInt(pos.offset, r.offset);
                p.setString(pos.value, r.val);
                yield transactionLog.append(rec);
            }
        };
    }

    record Pos(int txn, int file, int block, int offset, int value) {
        static Pos of(Page page) {
            int txn = Integer.BYTES;
            int file = txn + Integer.BYTES;
            int block = file + Page.maxLength(page.getString(file).length());
            int offset = block + Integer.BYTES;
            int value = offset + Integer.BYTES;
            return new Pos(txn, file, block, offset, value);
        }
        static Pos of(BlockId blockId) {
            int txn = Integer.BYTES;
            int file = txn + Integer.BYTES;
            int block = file + Page.maxLength(blockId.fileName().length());
            int offset = block + Integer.BYTES;
            int value = offset + Integer.BYTES;
            return new Pos(txn, file, block, offset, value);
        }
    }

}
