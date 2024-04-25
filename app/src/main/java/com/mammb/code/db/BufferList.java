package com.mammb.code.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BufferList {
    private final Map<BlockId, Buffer> buffers = new HashMap<>();
    private final List<BlockId> pins = new ArrayList<>();
    private final BufferPool bufferPool;

    public BufferList(BufferPool bufferPool) {
        this.bufferPool = bufferPool;
    }

    Buffer getBuffer(BlockId blk) {
        return buffers.get(blk);
    }

    void pin(BlockId blockId) {
        buffers.put(blockId, bufferPool.pin(blockId));
        pins.add(blockId);
    }

    void unpin(BlockId blockId) {
        bufferPool.unpin(buffers.get(blockId));
        pins.remove(blockId);
        if (!pins.contains(blockId)) {
            buffers.remove(blockId);
        }
    }

    void unpinAll() {
        pins.forEach(blockId -> bufferPool.unpin(buffers.get(blockId)));
        buffers.clear();
        pins.clear();
    }
}