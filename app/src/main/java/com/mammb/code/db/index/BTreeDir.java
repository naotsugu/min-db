package com.mammb.code.db.index;

import com.mammb.code.db.BlockId;
import com.mammb.code.db.Layout;
import com.mammb.code.db.Transaction;
import com.mammb.code.db.lang.DataBox;

public class BTreeDir {
    private final Transaction tx;
    private final Layout layout;
    private BTPage contents;
    private final String filename;

    BTreeDir(Transaction tx, BlockId blk, Layout layout) {
        this.tx = tx;
        this.layout = layout;
        contents = new BTPage(tx, blk, layout);
        filename = blk.fileName();
    }

    public void close() {
        contents.close();
    }

    public int search(DataBox<?> searchKey) {
        BlockId childblk = findChildBlock(searchKey);
        while (contents.getFlag() > 0) {
            contents.close();
            contents = new BTPage(tx, childblk, layout);
            childblk = findChildBlock(searchKey);
        }
        return childblk.number();
    }

    public void makeNewRoot(DirEntry e) {
        DataBox<?> firstVal = contents.getDataVal(0);
        int level = contents.getFlag();
        BlockId newBlock = contents.split(0, level); //ie, transfer all the records
        DirEntry oldRoot = new DirEntry(firstVal, newBlock.number());
        insertEntry(oldRoot);
        insertEntry(e);
        contents.setFlag(level+1);
    }

    public DirEntry insert(DirEntry e) {
        if (contents.getFlag() == 0) {
            return insertEntry(e);
        }
        BlockId childBlock = findChildBlock(e.dataVal());
        BTreeDir child = new BTreeDir(tx, childBlock, layout);
        DirEntry myEntry = child.insert(e);
        child.close();
        return (myEntry != null) ? insertEntry(myEntry) : null;
    }

    private DirEntry insertEntry(DirEntry e) {
        int newSlot = 1 + contents.findSlotBefore(e.dataVal());
        contents.insertDir(newSlot, e.dataVal(), e.blockNumber());
        if (!contents.isFull()) {
            return null;
        }
        // else page is full, so split it
        int level = contents.getFlag();
        int splitPos = contents.getNumRecs() / 2;
        DataBox<?> splitVal = contents.getDataVal(splitPos);
        BlockId newBlock = contents.split(splitPos, level);
        return new DirEntry(splitVal, newBlock.number());
    }

    private BlockId findChildBlock(DataBox<?> searchKey) {
        int slot = contents.findSlotBefore(searchKey);
        if (contents.getDataVal(slot+1).equals(searchKey)) {
            slot++;
        }
        int blknum = contents.getChildNum(slot);
        return new BlockId(filename, blknum);
    }
}
