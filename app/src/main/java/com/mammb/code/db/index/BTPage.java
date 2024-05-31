package com.mammb.code.db.index;

import com.mammb.code.db.BlockId;
import com.mammb.code.db.Layout;
import com.mammb.code.db.RId;
import com.mammb.code.db.Schema;
import com.mammb.code.db.Transaction;
import com.mammb.code.db.lang.DataBox;
import com.mammb.code.db.lang.FieldName;

public class BTPage {
    private Transaction tx;
    private BlockId current;
    private Layout layout;

    public BTPage(Transaction tx, BlockId current, Layout layout) {
        this.tx = tx;
        this.current = current;
        this.layout = layout;
        tx.pin(current);
    }

    public int findSlotBefore(DataBox<?> searchKey) {
        int slot = 0;
        while (slot < getNumRecs() && getDataVal(slot).compareLoose(searchKey) < 0) {
            slot++;
        }
        return slot - 1;
    }

    public void close() {
        if (current != null) {
            tx.unpin(current);
        }
        current = null;
    }

    public boolean isFull() {
        return slotPos(getNumRecs() + 1) >= tx.blockSize();
    }

    public BlockId split(int splitPos, int flag) {
        BlockId newBlockId = appendNew(flag);
        BTPage newPage = new BTPage(tx, newBlockId, layout);
        transferRecs(splitPos, newPage);
        newPage.setFlag(flag);
        newPage.close();
        return newBlockId;
    }

    public DataBox<?> getDataVal(int slot) {
        return getVal(slot, HashIndex.DATA_VAL);
    }

    public int getFlag() {
        return tx.getInt(current, 0);
    }

    public void setFlag(int val) {
        tx.setInt(current, 0, val, true);
    }

    public BlockId appendNew(int flag) {
        BlockId blk = tx.append(current.fileName());
        tx.pin(blk);
        format(blk, flag);
        return blk;
    }

    public void format(BlockId blk, int flag) {
        tx.setInt(blk, 0, flag, false);
        tx.setInt(blk, Integer.BYTES, 0, false);  // #records = 0
        int recSize = layout.slotSize();
        for (int pos= 2 * Integer.BYTES; pos + recSize <= tx.blockSize(); pos += recSize) {
            makeDefaultRecord(blk, pos);
        }
    }

    private void makeDefaultRecord(BlockId blk, int pos) {
        for (FieldName fieldName : layout.schema().fields()) {
            int offset = layout.offset(fieldName);
            if (layout.schema().type(fieldName) == java.sql.Types.INTEGER) {
                tx.setInt(blk, pos + offset, 0, false);
            } else {
                tx.setString(blk, pos + offset, "", false);
            }
        }
    }

    public int getChildNum(int slot) {
        return getInt(slot, HashIndex.BLOCK);
    }

    public void insertDir(int slot, DataBox<?> val, int blkNum) {
        insert(slot);
        setVal(slot, HashIndex.DATA_VAL, val);
        setInt(slot, HashIndex.BLOCK, blkNum);
    }

    // Methods called only by BTreeLeaf

    public RId getDataRid(int slot) {
        return new RId(getInt(slot, HashIndex.BLOCK), getInt(slot, HashIndex.ID));
    }

    public void insertLeaf(int slot, DataBox<?> val, RId rid) {
        insert(slot);
        setVal(slot, HashIndex.DATA_VAL, val);
        setInt(slot, HashIndex.BLOCK, rid.blockNum());
        setInt(slot, HashIndex.ID, rid.slot());
    }

    public void delete(int slot) {
        for (int i = slot + 1; i < getNumRecs(); i++) {
            copyRecord(i, i - 1);
        }
        setNumRecs(getNumRecs()-1);
    }

    public int getNumRecs() {
        return tx.getInt(current, Integer.BYTES);
    }

    // Private methods

    private int getInt(int slot, FieldName fieldName) {
        int pos = fldPos(slot, fieldName);
        return tx.getInt(current, pos);
    }

    private String getString(int slot, FieldName fieldName) {
        int pos = fldPos(slot, fieldName);
        return tx.getString(current, pos);
    }

    private DataBox<?> getVal(int slot, FieldName fieldName) {
        int type = layout.schema().type(fieldName);
        if (type == java.sql.Types.INTEGER) {
            return DataBox.of(getInt(slot, fieldName));
        } else {
            return DataBox.of(getString(slot, fieldName));
        }
    }

    private void setInt(int slot, FieldName fieldName, int val) {
        int pos = fldPos(slot, fieldName);
        tx.setInt(current, pos, val, true);
    }

    private void setString(int slot, FieldName fieldName, String val) {
        int pos = fldPos(slot, fieldName);
        tx.setString(current, pos, val, true);
    }

    private void setVal(int slot, FieldName fieldName, DataBox<?> val) {
        int type = layout.schema().type(fieldName);
        if (type == java.sql.Types.INTEGER) {
            setInt(slot, fieldName, ((DataBox.IntBox) val).val());
        } else {
            setString(slot, fieldName, ((DataBox.StrBox) val).val());
        }
    }

    private void setNumRecs(int n) {
        tx.setInt(current, Integer.BYTES, n, true);
    }

    private void insert(int slot) {
        for (int i = getNumRecs(); i > slot; i--)
            copyRecord(i - 1, i);
        setNumRecs(getNumRecs() + 1);
    }

    private void copyRecord(int from, int to) {
        Schema sch = layout.schema();
        for (FieldName fieldName : sch.fields()) {
            setVal(to, fieldName, getVal(from, fieldName));
        }
    }

    private void transferRecs(int slot, BTPage dest) {
        int destSlot = 0;
        while (slot < getNumRecs()) {
            dest.insert(destSlot);
            Schema sch = layout.schema();
            for (FieldName fieldName : sch.fields()) {
                dest.setVal(destSlot, fieldName, getVal(slot, fieldName));
            }
            delete(slot);
            destSlot++;
        }
    }

    private int fldPos(int slot, FieldName fieldName) {
        int offset = layout.offset(fieldName);
        return slotPos(slot) + offset;
    }

    private int slotPos(int slot) {
        int slotSize = layout.slotSize();
        return Integer.BYTES + Integer.BYTES + (slot * slotSize);
    }

}
