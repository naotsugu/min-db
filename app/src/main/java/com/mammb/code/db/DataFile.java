package com.mammb.code.db;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class DataFile {

    private Path baseDirectory;

    private int blockSize;

    private Map<String, RandomAccessFile> openFiles = new HashMap<>();


    public DataFile(Path baseDirectory, int blockSize) {

        this.baseDirectory = baseDirectory;
        this.blockSize = blockSize;

        createDirectories(baseDirectory);

        try (Stream<Path> stream = Files.list(baseDirectory)) {
            stream.filter(p -> baseDirectory.relativize(p).toString().startsWith("temp"))
                  .forEach(DataFile::delete);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void read(BlockId id, Page p) {
        try {
            RandomAccessFile file = getFile(id.fileName());
            file.seek((long) id.number() * blockSize);
            file.getChannel().read(p.contents());
        } catch (IOException e) {
            throw new RuntimeException("cannot read block " + id);
        }
    }

    public synchronized void write(BlockId id, Page p) {
        try {
            RandomAccessFile file = getFile(id.fileName());
            file.seek((long) id.number() * blockSize);
            file.getChannel().write(p.contents());
        } catch (IOException e) {
            throw new RuntimeException("cannot write block" + id);
        }
    }

    public synchronized BlockId append(String fileName) {
        BlockId id = new BlockId(fileName, length(fileName));
        byte[] bytes = new byte[blockSize];
        try {
            RandomAccessFile file = getFile(id.fileName());
            file.seek((long) id.number() * blockSize);
            file.write(bytes);
        } catch (IOException e) {
            throw new RuntimeException("cannot append block " + id);
        }
        return id;
    }

    public int length(String filename) {
        try {
            RandomAccessFile file = getFile(filename);
            return (int) (file.length() / blockSize);
        } catch (IOException e) {
            throw new RuntimeException("cannot access " + filename);
        }
    }

    private RandomAccessFile getFile(String fileName) {
        return openFiles.computeIfAbsent(fileName, name -> {
            try {
                return new RandomAccessFile(baseDirectory.resolve(name).toFile(), "rws");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static void createDirectories(Path path) {
        try {
            if (!Files.exists(path)) {
                Files.createDirectories(path);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void delete(Path path) {
        try {
            Files.delete(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
