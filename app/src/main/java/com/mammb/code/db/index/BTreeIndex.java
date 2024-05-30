package com.mammb.code.db.index;

import com.mammb.code.db.BlockId;
import com.mammb.code.db.Layout;
import com.mammb.code.db.RId;
import com.mammb.code.db.Schema;
import com.mammb.code.db.Transaction;
import com.mammb.code.db.lang.DataBox;

public class BTreeIndex implements Index {
    private Transaction tx;
    private Layout dirLayout, leafLayout;
    private String leaftbl;
    private BTreeLeaf leaf = null;
    private BlockId rootBlock;

    public BTreeIndex(Transaction tx, String idxName, Layout leafLayout) {
        this.tx = tx;
        // deal with the leaves
        leaftbl = idxName + "leaf";
        this.leafLayout = leafLayout;
        if (tx.size(BlockId.tailOf(leaftbl)) == 0) {
            BlockId blk = tx.append(leaftbl);
            BTPage node = new BTPage(tx, blk, leafLayout);
            node.format(blk, -1);
        }

        // deal with the directory
        Schema dirsch = new Schema(null);
        dirsch.add(HashIndex.BLOCK,   leafLayout.schema());
        dirsch.add(HashIndex.VAL, leafLayout.schema());
        String dirtbl = idxName + "dir";
        dirLayout = new Layout(dirsch);
        rootBlock = new BlockId(dirtbl, 0);
        if (tx.size(BlockId.tailOf(dirtbl)) == 0) {
            // create new root block
            tx.append(dirtbl);
            BTPage node = new BTPage(tx, rootBlock, dirLayout);
            node.format(rootBlock, 0);
            // insert initial directory entry
            int fldtype = dirsch.type(HashIndex.VAL);
            DataBox<?> minval = (fldtype == java.sql.Types.INTEGER) ?
                new DataBox.IntBox(Integer.MIN_VALUE) :
                new DataBox.StrBox("");
            node.insertDir(0, minval, 0);
            node.close();
        }
    }

    @Override
    public void beforeFirst(DataBox<?> searchKey) {
        close();
        BTreeDir root = new BTreeDir(tx, rootBlock, dirLayout);
        int blockNum = root.search(searchKey);
        root.close();
        BlockId leafBlock = new BlockId(leaftbl, blockNum);
        leaf = new BTreeLeaf(tx, leafBlock, leafLayout, searchKey);
    }

    @Override
    public boolean next() {
        return leaf.next();
    }

    @Override
    public RId getRid() {
        return leaf.getDataRid();
    }

    @Override
    public void insert(DataBox<?> val, RId datarid) {
        beforeFirst(val);
        DirEntry e = leaf.insert(datarid);
        leaf.close();
        if (e == null) {
            return;
        }
        BTreeDir root = new BTreeDir(tx, rootBlock, dirLayout);
        DirEntry e2 = root.insert(e);
        if (e2 != null) {
            root.makeNewRoot(e2);
        }
        root.close();
    }

    @Override
    public void delete(DataBox<?> val, RId datarid) {
        beforeFirst(val);
        leaf.delete(datarid);
        leaf.close();
    }

    @Override
    public void close() {
        if (leaf != null) {
            leaf.close();
        }
    }

    public static int searchCost(int numBlocks, int rpb) {
        return 1 + (int)(Math.log(numBlocks) / Math.log(rpb));
    }
}
