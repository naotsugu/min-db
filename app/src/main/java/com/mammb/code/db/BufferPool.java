package com.mammb.code.db;

public class BufferPool {
    private Buffer[] buffers;
    private int availableCount;

    public BufferPool(DataFile dataFile, int poolSize) {
        buffers = new Buffer[poolSize];
        availableCount = poolSize;
        for (int i = 0; i < poolSize; i++) {
            buffers[i] = new Buffer(dataFile);
        }
    }

    public synchronized int availableCount() {
        return availableCount;
    }

    public synchronized void flushAll(int txn) {
        for (Buffer buff : buffers) {
            if (buff.modifyingTx() == txn) {
                buff.flush();
            }
        }
    }

    public synchronized void unpin(Buffer buff) {
        buff.unpin();
        if (!buff.isPinned()) {
            availableCount++;
            notifyAll();
        }
    }

    public synchronized Buffer pin(BlockId blk) {
        try {
            long start = System.currentTimeMillis();
            Buffer buff = tryToPin(blk);
            while (buff == null) {
                if (System.currentTimeMillis() - start > 10_000) {
                    break;
                } else {
                    wait(1_000);
                }
                buff = tryToPin(blk);
            }
            if (buff == null) {
                throw new RuntimeException("bufferAbort");
            }
            return buff;
        } catch (InterruptedException e) {
            throw new RuntimeException("bufferAbort");
        }
    }

    private Buffer tryToPin(BlockId blockId) {
        Buffer buff = findExistingBuffer(blockId);
        if (buff == null) {
            buff = chooseUnpinnedBuffer();
            if (buff == null) {
                return null;
            }
            buff.assignToBlock(blockId);
        }
        if (!buff.isPinned()) {
            availableCount--;
        }
        buff.pin();
        return buff;
    }

    private Buffer findExistingBuffer(BlockId blockId) {
        for (Buffer buff : buffers) {
            BlockId b = buff.blockId();
            if (b != null && b.equals(blockId)) {
                return buff;
            }
        }
        return null;
    }

    private Buffer chooseUnpinnedBuffer() {
        for (Buffer buff : buffers) {
            if (!buff.isPinned()) {
                return buff;
            }
        }
        return null;
    }

}
