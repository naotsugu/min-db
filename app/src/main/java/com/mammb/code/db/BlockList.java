package com.mammb.code.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BlockList {
    private final Map<BlockId, Block> buffers = new HashMap<>();
    private final List<BlockId> pins = new ArrayList<>();
    private final BlockPool blockPool;

    public BlockList(BlockPool blockPool) {
        this.blockPool = blockPool;
    }

    Block getBuffer(BlockId blockId) {
        return buffers.get(blockId);
    }

    void pin(BlockId blockId) {
        buffers.put(blockId, blockPool.pin(blockId));
        pins.add(blockId);
    }

    void unpin(BlockId blockId) {
        blockPool.unpin(buffers.get(blockId));
        pins.remove(blockId);
        if (!pins.contains(blockId)) {
            buffers.remove(blockId);
        }
    }

    void unpinAll() {
        pins.forEach(blockId -> blockPool.unpin(buffers.get(blockId)));
        buffers.clear();
        pins.clear();
    }
}
