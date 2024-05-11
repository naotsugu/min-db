package com.mammb.code.db;

public record FieldName(String val) {
    public static FieldName of(String val) {
        return new FieldName(val);
    }
}
