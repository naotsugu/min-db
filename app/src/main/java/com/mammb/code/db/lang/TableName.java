package com.mammb.code.db.lang;

public record TableName(String val) {
    public static TableName of(String val) {
        return new TableName(val);
    }
}

