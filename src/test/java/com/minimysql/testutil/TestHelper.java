package com.minimysql.testutil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * 测试工具类
 *
 * 提供测试常用的工具方法，避免代码重复。
 */
public final class TestHelper {

    private TestHelper() {
        // 工具类，禁止实例化
    }

    /**
     * 递归删除目录
     *
     * @param directory 要删除的目录
     */
    public static void deleteDirectory(File directory) {
        if (directory == null || !directory.exists()) {
            return;
        }

        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        directory.delete();
    }

    /**
     * 递归删除目录（NIO版本）
     *
     * @param path 要删除的目录路径
     * @throws IOException 如果删除失败
     */
    public static void deleteDirectory(Path path) throws IOException {
        if (path == null || !Files.exists(path)) {
            return;
        }

        Files.walk(path)
                .sorted((a, b) -> b.compareTo(a)) // 反向排序，先删除文件
                .forEach(p -> {
                    try {
                        Files.delete(p);
                    } catch (IOException e) {
                        // 忽略删除失败
                    }
                });
    }

    /**
     * 清理测试目录（如果存在）
     *
     * @param dirPath 目录路径
     */
    public static void cleanupTestDir(String dirPath) {
        File dir = new File(dirPath);
        if (dir.exists()) {
            deleteDirectory(dir);
        }
    }

    /**
     * 清理测试目录（如果存在）
     *
     * @param dir 目录
     */
    public static void cleanupTestDir(File dir) {
        if (dir != null && dir.exists()) {
            deleteDirectory(dir);
        }
    }
}
