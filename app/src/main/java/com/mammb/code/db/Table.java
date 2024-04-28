package com.mammb.code.db;

import java.util.function.Function;

public class Table {

    private static final Function<Schema, String> toFileName = s -> s.tableName() + ".tbl";

    private final Transaction tx;
    private final Layout layout;
    private RecordPage recordPage;
    private int currentSlot;

    public Table(Transaction tx, Layout layout) {
        this.tx = tx;
        this.layout = layout;
        String fileName = toFileName.apply(layout.schema());
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
        BlockId blockId = BlockId.of(toFileName.apply(layout.schema()), 0);
        recordPage = new RecordPage(tx, blockId, layout);
        currentSlot = -1;
    }

    private void moveToNewBlock() {
        close();
        BlockId blockId = BlockId.tailOf(toFileName.apply(layout.schema()));
        tx.append(blockId);
        recordPage = new RecordPage(tx, blockId, layout);
        recordPage.format();
        currentSlot = -1;
    }

}
