package com.kafka.jr.util;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * File utilities for the broker.
 */
public class FileUtils {
    /**
     * Ensure a directory exists, creating it if necessary.
     */
    public static void ensureDirectory(Path path) throws Exception {
        Files.createDirectories(path);
    }

    /**
     * Get the size of a file in bytes.
     */
    public static long getFileSize(Path path) throws Exception {
        return Files.size(path);
    }
}
