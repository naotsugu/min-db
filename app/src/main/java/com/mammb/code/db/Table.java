package com.mammb.code.db;

public class Table {

    private final Transaction tx;
    private final Layout layout;
    private RecordPage recordPage;
    private int currentSlot;
    private final String fileName;

    public Table(Transaction tx, Layout layout) {
        this.tx = tx;
        this.layout = layout;
        this.fileName = layout.schema().tableName() + ".tbl";
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
    }
    public void close() {
    }

    public void setInt(FieldName name, int val) {
    }

    public void setString(FieldName name, String val) {
    }

    public int getInt(FieldName name) {
        return 0;
    }

    public String getString(FieldName name) {
        return null;
    }

    public Schema schema() {
        return layout.schema();
    }

    public Layout layout() {
        return layout;
    }

    private void moveToBlock(int n) {
        close();
        BlockId blockId = BlockId.of(fileName, 0);
        recordPage = new RecordPage(tx, blockId, layout);
        currentSlot = -1;
    }

    private void moveToNewBlock() {
        close();
        BlockId blockId = BlockId.tailOf(fileName);
        tx.append(blockId);
        recordPage = new RecordPage(tx, blockId, layout);
        recordPage.format();
        currentSlot = -1;
    }

    private boolean atLastBlock() {
        return recordPage.block().number() == tx.size(BlockId.tailOf(fileName)) - 1;
    }

}
