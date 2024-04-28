package com.mammb.code.db;

public record BlockId(String fileName, int number) {
    public static BlockId of(String fileName, int number) {
        return new BlockId(fileName, number);
    }
    public static BlockId of(String fileName) {
        return new BlockId(fileName, 0);
    }
    public static BlockId tailOf(String fileName) {
        return new BlockId(fileName, -1);
    }
}
