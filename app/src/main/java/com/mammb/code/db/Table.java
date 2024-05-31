package com.mammb.code.db;

import com.mammb.code.db.lang.DataBox;
import com.mammb.code.db.lang.FieldName;
import com.mammb.code.db.lang.TableName;
import com.mammb.code.db.query.UpdateScan;

public class Table implements UpdateScan {

    private final Transaction tx;
    private final Layout layout;
    private RecordPage recordPage;
    private int currentSlot;
    private final String fileName;

    public Table(Transaction tx, Layout layout) {
        this(tx, layout.schema().tableName(), layout);
    }

    private Table(Transaction tx, TableName tableName, Layout layout) {
        this.tx = tx;
        this.layout = layout;
        this.fileName = tableName.val() + ".tbl";
        if (tx.size(BlockId.tailOf(fileName)) == 0) {
            moveToNewBlock();
        } else {
            moveToBlock(0);
        }
    }

    public void beforeFirst() {
        moveToBlock(0);
    }

    public boolean next() {
        currentSlot = recordPage.nextAfter(currentSlot);
        while (currentSlot < 0) {
            if (atLastBlock()) {
                return false;
            }
            moveToBlock(recordPage.block().number() + 1);
            currentSlot = recordPage.nextAfter(currentSlot);
        }
        return true;
    }

    public void insert() {
        currentSlot = recordPage.insertAfter(currentSlot);
        while (currentSlot < 0) {
            if (atLastBlock()) {
                moveToNewBlock();
            } else {
                moveToBlock(recordPage.block().number() + 1);
            }
            currentSlot = recordPage.insertAfter(currentSlot);
        }
    }

    public void delete() {
        recordPage.delete(currentSlot);
    }

    public void close() {
        if (recordPage != null) {
            tx.unpin(recordPage.block());
        }
    }

    public void setInt(FieldName name, int val) {
        recordPage.setInt(currentSlot, name, val);
    }

    public void setString(FieldName name, String val) {
        recordPage.setString(currentSlot, name, val);
    }

    public int getInt(FieldName name) {
        return recordPage.getInt(currentSlot, name);
    }

    public String getString(FieldName name) {
        return recordPage.getString(currentSlot, name);
    }

    public DataBox<?> getVal(FieldName name) {
        if (layout.schema().type(name) == java.sql.Types.INTEGER) {
            return new DataBox.IntBox(getInt(name));
        } else {
            return new DataBox.StrBox(getString(name));
        }
    }

    public void setVal(FieldName name, DataBox<?> val) {
        if (layout.schema().type(name) == java.sql.Types.INTEGER) {
            setInt(name, (Integer) val.val());
        } else {
            setString(name, (String) val.val());
        }
    }

    @Override
    public boolean hasField(FieldName name) {
        return layout.schema().hasField(name);
    }

    public Schema schema() {
        return layout.schema();
    }

    public Layout layout() {
        return layout;
    }

    @Override
    public void moveToRid(RId rid) {
        close();
        BlockId blockId = BlockId.of(fileName, rid.blockNum());
        recordPage = new RecordPage(tx, blockId, layout);
        currentSlot = rid.slot();
    }

    @Override
    public RId getRid() {
        return new RId(recordPage.block().number(), currentSlot);
    }

    private void moveToBlock(int n) {
        close();
        BlockId blockId = BlockId.of(fileName, n);
        recordPage = new RecordPage(tx, blockId, layout);
        currentSlot = -1;
    }

    private void moveToNewBlock() {
        close();
        BlockId blk = tx.append(fileName);
        recordPage = new RecordPage(tx, blk, layout);
        recordPage.format();
        currentSlot = -1;
    }

    private boolean atLastBlock() {
        return recordPage.block().number() == tx.size(BlockId.tailOf(fileName)) - 1;
    }

}
