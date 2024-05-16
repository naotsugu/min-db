package com.mammb.code.db.lang;

public record IdxName(String val) {
    public static IdxName of(String val) {
        return new IdxName(val);
    }
}

