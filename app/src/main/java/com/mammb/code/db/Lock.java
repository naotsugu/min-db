package com.mammb.code.db;

import java.util.HashMap;
import java.util.Map;

public class Lock {
    private static LockTable lockTable = new LockTable();
    private Map<BlockId, String> locks  = new HashMap<>();

    public void sLock(BlockId blockId) {
        if (locks.get(blockId) == null) {
            lockTable.sLock(blockId);
            locks.put(blockId, "S");
        }
    }

    public void xLock(BlockId blockId) {
        if (!hasXLock(blockId)) {
            sLock(blockId);
            lockTable.xLock(blockId);
            locks.put(blockId, "X");
        }
    }

    public void release() {
        for (BlockId blk : locks.keySet()) {
            lockTable.unlock(blk);
        }
        locks.clear();
    }

    private boolean hasXLock(BlockId blockId) {
        String type = locks.get(blockId);
        return type != null && type.equals("X");
    }

}
