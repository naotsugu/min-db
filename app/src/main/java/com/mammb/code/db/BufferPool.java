package com.mammb.code.db;

public class BufferPool {
    public static final int BUFFER_SIZE = 8;
    private final Block[] pool;
    private int availableCount;

    public BufferPool(DataFile dataFile, TransactionLog txLog, int poolSize) {
        pool = new Block[poolSize];
        availableCount = poolSize;
        for (int i = 0; i < poolSize; i++) {
            pool[i] = new Block(dataFile, txLog);
        }
    }

    public BufferPool(DataFile dataFile, TransactionLog txLog) {
        this(dataFile, txLog, BUFFER_SIZE);
    }

    public synchronized int availableCount() {
        return availableCount;
    }

    public synchronized void flushAll(int txn) {
        for (Block buff : pool) {
            if (buff.modifyingTx() == txn) {
                buff.flush();
            }
        }
    }

    public synchronized void unpin(Block block) {
        block.unpin();
        if (!block.isPinned()) {
            availableCount++;
            notifyAll();
        }
    }

    public synchronized Block pin(BlockId blk) {
        try {
            long start = System.currentTimeMillis();
            Block block = tryToPin(blk);
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

    private Block tryToPin(BlockId blockId) {
        Block block = findExistingBuffer(blockId);
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

    private Block findExistingBuffer(BlockId blockId) {
        for (Block buff : pool) {
            BlockId b = buff.blockId();
            if (b != null && b.equals(blockId)) {
                return buff;
            }
        }
        return null;
    }

    private Block chooseUnpinnedBuffer() {
        for (Block buff : pool) {
            if (!buff.isPinned()) {
                return buff;
            }
        }
        return null;
    }

}
