package com.mammb.code.db;

import java.util.Iterator;

public class TransactionLog {
    private DataFile dataFile;
    private String name;
    private Page logPage;
    private BlockId currentBlock;
    private int latestLSN = 0;
    private int lastSavedLSN = 0;

    public TransactionLog(DataFile dataFile, String name) {
        this.dataFile = dataFile;
        this.name = name;
        this.logPage = new Page(new byte[dataFile.blockSize()]);

        int logSize = dataFile.length(name);
        if (logSize == 0) {
            currentBlock = appendNewBlock();
        } else {
            currentBlock = new BlockId(name, logSize - 1);
            dataFile.read(currentBlock, logPage);
        }

    }
    public void flush(int lsn) {
        if (lsn >= lastSavedLSN) {
            flush();
        }
    }

    public synchronized int append(byte[] logrec) {
        int boundary = logPage.getInt(0);
        int recSize = logrec.length;
        int bytesNeeded = recSize + Integer.BYTES;
        if (boundary - bytesNeeded < Integer.BYTES) {
            // the log record doesn't fit, so move to the next block.
            flush();
            currentBlock = appendNewBlock();
            boundary = logPage.getInt(0);
        }
        int recPos = boundary - bytesNeeded;

        logPage.setBytes(recPos, logrec);
        logPage.setInt(0, recPos); // the new boundary
        latestLSN += 1;
        return latestLSN;
    }

    public Iterator<byte[]> iterator() {
        flush();
        return new Iterator<>() {
            private BlockId blockId = currentBlock;
            private Page page = new Page(new byte[dataFile.blockSize()]);
            private int currentPos;
            private int boundary;

            @Override
            public boolean hasNext() {
                return currentPos < dataFile.blockSize() || blockId.number() > 0;
            }
            @Override
            public byte[] next() {
                if (currentPos == dataFile.blockSize()) {
                    blockId = new BlockId(name, blockId.number() - 1);
                    dataFile.read(blockId, page);
                    boundary = page.getInt(0);
                    currentPos = boundary;
                }
                byte[] rec = page.getBytes(currentPos);
                currentPos += Integer.BYTES + rec.length;
                return rec;
            }
        };
    }

    private void flush() {
        dataFile.write(currentBlock, logPage);
        lastSavedLSN = latestLSN;
    }

    private BlockId appendNewBlock() {
        BlockId blockId = dataFile.append(name);
        logPage.setInt(0, dataFile.blockSize());
        dataFile.write(blockId, logPage);
        return blockId;
    }



}
