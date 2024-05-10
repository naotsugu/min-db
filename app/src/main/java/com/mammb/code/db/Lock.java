package com.mammb.code.db;

import java.util.HashMap;
import java.util.Map;

public class Lock {
    private enum Mode { SHARED, EXCLUSIVE }
    private static LockTable lockTable = new LockTable();
    private Map<BlockId, Mode> locks  = new HashMap<>();

    public void sLock(BlockId blockId) {
        if (locks.get(blockId) == null) {
            lockTable.sLock(blockId);
            locks.put(blockId, Mode.SHARED);
        }
    }

    public void xLock(BlockId blockId) {
        if (!hasXLock(blockId)) {
            sLock(blockId);
            lockTable.xLock(blockId);
            locks.put(blockId, Mode.EXCLUSIVE);
        }
    }

    public void release() {
        for (BlockId blockId : locks.keySet()) {
            lockTable.unlock(blockId);
        }
        locks.clear();
    }

    private boolean hasXLock(BlockId blockId) {
        return locks.get(blockId) == Mode.EXCLUSIVE;
    }

}
