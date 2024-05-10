package com.mammb.code.db;

import java.util.Arrays;

public class BufferPool {
    public static final int BUFFER_SIZE = 8;
    private BlockBuffer[] pool;
    private int availableCount;

    public BufferPool(DataFile dataFile, TransactionLog txLog, int poolSize) {
        pool = new BlockBuffer[poolSize];
        availableCount = poolSize;
        for (int i = 0; i < poolSize; i++) {
            pool[i] = new BlockBuffer(dataFile, txLog);
        }
    }

    public BufferPool(DataFile dataFile, TransactionLog txLog) {
        this(dataFile, txLog, BUFFER_SIZE);
    }

    public synchronized int availableCount() {
        return availableCount;
    }

    public synchronized void flushAll(int txn) {
        for (BlockBuffer buff : pool) {
            if (buff.modifyingTx() == txn) {
                buff.flush();
            }
        }
    }

    public synchronized void unpin(BlockBuffer block) {
        block.unpin();
        if (!block.isPinned()) {
            availableCount++;
            notifyAll();
        }
    }

    public synchronized BlockBuffer pin(BlockId blk) {
        try {
            long start = System.currentTimeMillis();
            BlockBuffer block = tryToPin(blk);
            while (block == null) {
                if (System.currentTimeMillis() - start > 10_000) {
                    break;
                } else {
                    wait(1_000);
                }
                block = tryToPin(blk);
            }
            if (block == null) {
                throw new RuntimeException("bufferAbort");
            }
            return block;
        } catch (InterruptedException e) {
            throw new RuntimeException("bufferAbort");
        }
    }

    private BlockBuffer tryToPin(BlockId blockId) {
        BlockBuffer block = findExistingBuffer(blockId);
        if (block == null) {
            block = chooseUnpinnedBuffer();
            if (block == null) {
                return null;
            }
            block.assignToBlock(blockId);
        }
        if (!block.isPinned()) {
            availableCount--;
        }
        block.pin();
        return block;
    }

    private BlockBuffer findExistingBuffer(BlockId blockId) {
        for (BlockBuffer buff : pool) {
            BlockId b = buff.blockId();
            if (b != null && b.equals(blockId)) {
                return buff;
            }
        }
        return null;
    }

    private BlockBuffer chooseUnpinnedBuffer() {
        for (BlockBuffer buff : pool) {
            if (!buff.isPinned()) {
                return buff;
            }
        }
        return null;
    }

}
