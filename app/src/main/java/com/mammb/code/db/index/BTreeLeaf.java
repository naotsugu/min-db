package com.mammb.code.db.index;

import com.mammb.code.db.BlockId;
import com.mammb.code.db.Layout;
import com.mammb.code.db.RId;
import com.mammb.code.db.Transaction;
import com.mammb.code.db.lang.DataBox;

public class BTreeLeaf {
    private final Transaction tx;
    private final Layout layout;
    private final DataBox<?> searchKey;
    private BTPage contents;
    private int currentSlot;
    private final String filename;

    public BTreeLeaf(Transaction tx, BlockId blk, Layout layout, DataBox<?> searchKey) {
        this.tx = tx;
        this.layout = layout;
        this.searchKey = searchKey;
        contents = new BTPage(tx, blk, layout);
        currentSlot = contents.findSlotBefore(searchKey);
        filename = blk.fileName();
    }

    public void close() {
        contents.close();
    }

    public boolean next() {
        currentSlot++;
        if (currentSlot >= contents.getNumRecs()) {
            return tryOverflow();
        } else if (contents.getDataVal(currentSlot).equals(searchKey)) {
            return true;
        } else {
            return tryOverflow();
        }
    }

    public RId getDataRid() {
        return contents.getDataRid(currentSlot);
    }

    public void delete(RId datarid) {
        while (next()) {
            if (getDataRid().equals(datarid)) {
                contents.delete(currentSlot);
                return;
            }
        }
    }

    public DirEntry insert(RId datarid) {
        if (contents.getFlag() >= 0 && contents.getDataVal(0).compareLoose(searchKey) > 0) {
            DataBox<?> firstVal = contents.getDataVal(0);
            BlockId newBlock = contents.split(0, contents.getFlag());
            currentSlot = 0;
            contents.setFlag(-1);
            contents.insertLeaf(currentSlot, searchKey, datarid);
            return new DirEntry(firstVal, newBlock.number());
        }

        currentSlot++;
        contents.insertLeaf(currentSlot, searchKey, datarid);
        if (!contents.isFull()) {
            return null;
        }
        // else page is full, so split it
        DataBox<?> firstKey = contents.getDataVal(0);
        DataBox<?> lastKey  = contents.getDataVal(contents.getNumRecs() - 1);
        if (lastKey.equals(firstKey)) {
            // create an overflow block to hold all but the first record
            BlockId newblk = contents.split(1, contents.getFlag());
            contents.setFlag(newblk.number());
            return null;
        }
        else {
            int splitpos = contents.getNumRecs() / 2;
            DataBox<?> splitkey = contents.getDataVal(splitpos);
            if (splitkey.equals(firstKey)) {
                // move right, looking for the next key
                while (contents.getDataVal(splitpos).equals(splitkey)) {
                    splitpos++;
                }
                splitkey = contents.getDataVal(splitpos);
            }
            else {
                // move left, looking for first entry having that key
                while (contents.getDataVal(splitpos - 1).equals(splitkey)) {
                    splitpos--;
                }
            }
            BlockId newBlock = contents.split(splitpos, -1);
            return new DirEntry(splitkey, newBlock.number());
        }
    }

    private boolean tryOverflow() {
        DataBox<?> firstKey = contents.getDataVal(0);
        int flag = contents.getFlag();
        if (!searchKey.equals(firstKey) || flag < 0) {
            return false;
        }
        contents.close();
        BlockId nextBlock = new BlockId(filename, flag);
        contents = new BTPage(tx, nextBlock, layout);
        currentSlot = 0;
        return true;
    }

}
