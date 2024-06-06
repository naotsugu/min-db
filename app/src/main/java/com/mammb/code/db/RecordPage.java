package com.mammb.code.db;

import com.mammb.code.db.lang.FieldName;

public class RecordPage {
    public static final int EMPTY = 0;
    public static final int USED = 1;
    private final Transaction tx;
    private final BlockId blockId;
    private final Layout layout;

    public RecordPage(Transaction tx, BlockId blockId, Layout layout) {
        this.tx = tx;
        this.blockId = blockId;
        this.layout = layout;
        tx.pin(blockId);
    }

    public int getInt(int slot, FieldName name) {
        int pos = offset(slot) + layout.offset(name);
        return tx.getInt(blockId, pos);
    }

    public String getString(int slot, FieldName name) {
        int pos = offset(slot) + layout.offset(name);
        return tx.getString(blockId, pos);
    }

    public void setInt(int slot, FieldName name, int val) {
        int pos = offset(slot) + layout.offset(name);
        tx.setInt(blockId, pos, val, true);
    }

    public void setString(int slot, FieldName name, String val) {
        int pos = offset(slot) + layout.offset(name);
        tx.setString(blockId, pos, val, true);
    }

    public void delete(int slot) {
        setFlag(slot, EMPTY);
    }

    public void format() {
        int slot = 0;
        while (isValidSlot(slot)) {
            tx.setInt(blockId, offset(slot), EMPTY, false);
            Schema schema = layout.schema();
            for (FieldName name : schema.fields()) {
                int pos = offset(slot) + layout.offset(name);
                if (schema.type(name) == java.sql.Types.INTEGER) {
                    tx.setInt(blockId, pos, 0, false);
                } else {
                    tx.setString(blockId, pos, "", false);
                }
            }
            slot++;
        }
    }

    public int nextAfter(int slot) {
        return searchAfter(slot, USED);
    }

    public int insertAfter(int slot) {
        int newSlot = searchAfter(slot, EMPTY);
        if (newSlot >= 0) {
            setFlag(newSlot, USED);
        }
        return newSlot;
    }

    public BlockId block() {
        return blockId;
    }

    private void setFlag(int slot, int flag) {
        tx.setInt(blockId, offset(slot), flag, true);
    }

    private int searchAfter(int slot, int flag) {
        slot++;
        while (isValidSlot(slot)) {
            if (tx.getInt(blockId, offset(slot)) == flag) {
                return slot;
            }
            slot++;
        }
        return -1;
    }

    private boolean isValidSlot(int slot) {
        return offset(slot+1) <= tx.blockSize();
    }

    private int offset(int slot) {
        return slot * layout.slotSize();
    }


}
