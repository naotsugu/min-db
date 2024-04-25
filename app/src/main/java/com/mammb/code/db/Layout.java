package com.mammb.code.db;

import java.util.HashMap;
import java.util.Map;

// Description of the structure of a record.
public class Layout {
    private Schema schema;
    private Map<FieldName, Integer> offsets = new HashMap<>();
    private int slotSize;

    public Layout(Schema schema) {
        this.schema = schema;
        int pos = Integer.BYTES;
        for (FieldName name : schema.fields()) {
            offsets.put(name, pos);
            pos += lengthInBytes(name);
        }
        slotSize = pos;
    }

    public Layout(Schema schema, Map<FieldName, Integer> offsets, int slotSize) {
        this.schema = schema;
        this.offsets = offsets;
        this.slotSize = slotSize;
    }

    public Schema schema() {
        return schema;
    }

    public int offset(FieldName name) {
        return offsets.get(name);
    }

    public int slotSize() {
        return slotSize;
    }

    private int lengthInBytes(FieldName name) {
        int type = schema.type(name);
        if (type == java.sql.Types.INTEGER) {
            return Integer.BYTES;
        } else if (type == java.sql.Types.VARCHAR) {
            return Page.maxLength(schema.length(name));
        } else {
            throw new RuntimeException();
        }
    }

}