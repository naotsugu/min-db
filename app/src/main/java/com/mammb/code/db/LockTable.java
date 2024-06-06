package com.mammb.code.db;

import java.util.HashMap;
import java.util.Map;

public class LockTable {

    private static final long MAX_TIME = 10000; // 10 seconds
    private final Map<BlockId, Integer> locks = new HashMap<>();

    public synchronized void sLock(BlockId blockId) {
        try {
            long timestamp = System.currentTimeMillis();
            while (hasXLock(blockId) && !waitingTooLong(timestamp)) {
                wait(MAX_TIME);
            }
            if (hasXLock(blockId)) {
                throw new RuntimeException("lock abort");
            }
            int val = getLockVal(blockId);  // will not be negative
            locks.put(blockId, val + 1);
        } catch(InterruptedException e) {
            throw new RuntimeException("lock abort");
        }
    }

    synchronized void xLock(BlockId blockId) {
        try {
            long timestamp = System.currentTimeMillis();
            while (hasOtherSLocks(blockId) && !waitingTooLong(timestamp)) {
                wait(MAX_TIME);
            }
            if (hasOtherSLocks(blockId)) {
                throw new RuntimeException("lock abort");
            }
            locks.put(blockId, -1);
        } catch(InterruptedException e) {
            throw new RuntimeException("lock abort");
        }
    }

    synchronized void unlock(BlockId blockId) {
        int val = getLockVal(blockId);
        if (val > 1) {
            locks.put(blockId, val - 1);
        } else {
            locks.remove(blockId);
            notifyAll();
        }
    }

    private boolean hasXLock(BlockId blockId) {
        return getLockVal(blockId) < 0;
    }

    private boolean hasOtherSLocks(BlockId blockId) {
        return getLockVal(blockId) > 1;
    }

    private boolean waitingTooLong(long start) {
        return System.currentTimeMillis() - start > MAX_TIME;
    }

    private int getLockVal(BlockId blockId) {
        Integer val = locks.get(blockId);
        return (val == null) ? 0 : val;
    }

}
