package com.mammb.code.db;

public sealed interface LogRecord {

    record CheckPoint() implements LogRecord {}
    record Start(int txn) implements LogRecord {}
    record Commit(int txn) implements LogRecord {}
    record Rollback(int txn) implements LogRecord {}
    record SetInt(int txn, int offset, int val, BlockId blockId) implements LogRecord {}
    record SetString(int txn, int offset, String val, BlockId blockId) implements LogRecord {}


    static LogRecord createLogRecord(byte[] bytes) {
        Page p = new Page(bytes);
        return switch (p.getInt(0)) {
            case 0 -> new CheckPoint();
            case 1 -> new Start(p.getInt(Integer.BYTES));
            case 2 -> new Commit(p.getInt(Integer.BYTES));
            case 3 -> new Rollback(p.getInt(Integer.BYTES));
            case 4 -> {
                Pos pos = Pos.of(p);
                yield new SetInt(p.getInt(pos.txn), p.getInt(pos.offset), p.getInt(pos.value),
                    new BlockId(p.getString(pos.file), p.getInt(pos.block)));
            }
            case 5 -> {
                Pos pos = Pos.of(p);
                yield new SetString(p.getInt(pos.txn), p.getInt(pos.offset), p.getString(pos.value),
                    new BlockId(p.getString(pos.file), p.getInt(pos.block)));
            }
            default -> throw new RuntimeException();
        };
    }

    record Pos(int txn, int file, int block, int offset, int value) {
        static Pos of(Page page) {
            int tpos = Integer.BYTES;
            int fpos = tpos + Integer.BYTES;
            int bpos = fpos + Page.maxLength(page.getString(fpos).length());
            int opos = bpos + Integer.BYTES;
            int vpos = opos + Integer.BYTES;
            return new Pos(tpos, fpos, bpos, opos, vpos);
        }
    }

}
