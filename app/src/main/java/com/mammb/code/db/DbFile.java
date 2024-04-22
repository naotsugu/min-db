package com.mammb.code.db;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class DbFile {
    private Path baseDirectory;
    private int blockSize;
    private Map<String, RandomAccessFile> openFiles = new HashMap<>();

    public DbFile(Path baseDirectory, int blockSize) {
        this.baseDirectory = baseDirectory;
        this.blockSize = blockSize;

        if (!Files.exists(baseDirectory)) {
            try {
                Files.createDirectories(baseDirectory);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        try {
            Files.list(baseDirectory)
                .filter(p -> baseDirectory.relativize(p).toString().startsWith("temp"))
                .forEach(DbFile::delete);
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
