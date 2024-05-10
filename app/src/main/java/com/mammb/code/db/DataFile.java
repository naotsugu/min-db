package com.mammb.code.db;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public class DataFile {
    public static final int BLOCK_SIZE = 400;
    private final Path root;
    private final int blockSize;
    private final Map<String, RandomAccessFile> openFiles = new HashMap<>();


    public DataFile(Path root, int blockSize) {
        this.root = root;
        this.blockSize = blockSize;

        createDirectories(root);

        loanList(root, list -> {
            list.filter(p -> root.relativize(p).toString().startsWith("temp"))
                .forEach(DataFile::delete);
            return null;
        });
    }

    public DataFile(Path root) {
        this(root, BLOCK_SIZE);
    }

    public synchronized void read(BlockId id, ByteBuffer byteBuffer) {
        try {
            RandomAccessFile file = getFile(id.fileName());
            file.seek((long) id.number() * blockSize);
            byteBuffer.readFrom(file.getChannel());
        } catch (IOException e) {
            throw new RuntimeException("cannot read block " + id);
        }
    }

    public synchronized void write(BlockId id, ByteBuffer byteBuffer) {
        try {
            RandomAccessFile file = getFile(id.fileName());
            file.seek((long) id.number() * blockSize);
            byteBuffer.writeTo(file.getChannel());
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

    public boolean isEmpty() {
        return loanList(root, s -> s.findAny().isEmpty());
    }

    private RandomAccessFile getFile(String fileName) {
        return openFiles.computeIfAbsent(fileName, name -> {
            try {
                return new RandomAccessFile(root.resolve(name).toFile(), "rws");
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

    private static <R> R loanList(Path path, Function<Stream<Path>, R> fn) {
        try (Stream<Path> stream = Files.list(path)) {
            return fn.apply(stream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public int blockSize() {
        return blockSize;
    }
}
