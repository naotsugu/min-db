package com.mammb.code.db;

public sealed interface LogRecord {

    record CheckPoint() implements LogRecord {}
    record Start(int txn) implements LogRecord {}
    record Commit(int txn) implements LogRecord {}
    record Rollback(int txn) implements LogRecord {}
    record SetInt(int txn) implements LogRecord {}
    record SetString(int txn) implements LogRecord {}


    static LogRecord createLogRecord(byte[] bytes) {
        Page p = new Page(bytes);
        return switch (p.getInt(0)) {
            case 0 -> new CheckPoint();
            case 1 -> new Start(p.getInt(Integer.BYTES));
            case 2 -> new Commit(p.getInt(Integer.BYTES));
            case 3 -> new Rollback(p.getInt(Integer.BYTES));
            case 4 -> new SetInt(p.getInt(Integer.BYTES));
            case 5 -> new SetString(p.getInt(Integer.BYTES));
            default -> throw new RuntimeException();
        };
    }
}
