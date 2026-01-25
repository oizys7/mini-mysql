package com.minimysql.storage;

import com.minimysql.CommonConstant;
import com.minimysql.storage.impl.InnoDBStorageEngine;

/**
 * StorageEngineFactory - 存储引擎工厂
 *
 * 采用工厂模式+策略模式，支持根据配置创建不同的存储引擎实例。
 *
 * MySQL对应:
 * - MySQL的存储引擎插件系统
 * - CREATE TABLE ... ENGINE=InnoDB 语法
 *
 * 设计原则:
 * - "Good taste": 简单的工厂方法，没有复杂的依赖注入
 * - 策略模式: 通过EngineType枚举选择引擎实现
 * - 单一职责: 只负责创建引擎实例，不管理生命周期
 *
 * 使用示例:
 * <pre>
 * // 创建InnoDB引擎（默认配置）
 * StorageEngine engine = StorageEngineFactory.createEngine(EngineType.INNODB, 1024);
 *
 * // 创建InnoDB引擎（启用元数据持久化）
 * StorageEngine engine = StorageEngineFactory.createEngine(
 *     EngineType.INNODB,
 *     1024,
 *     true,
 *     "./data"
 * );
 * </pre>
 *
 * 未来扩展:
 * - EngineType.MEMORY → MemoryStorageEngine
 * - EngineType.CSV → CSVStorageEngine
 * - 支持自定义引擎配置
 */
public class StorageEngineFactory {

    /**
     * 存储引擎类型枚举
     *
     * 定义支持的存储引擎类型。
     * 当前只实现InnoDB，预留Memory、CSV等引擎。
     */
    public enum EngineType {
        /**
         * InnoDB存储引擎（默认）
         *
         * 特点:
         * - 支持事务（未来实现）
         * - 支持外键（未来实现）
         * - 聚簇索引
         * - 行级锁（未来实现）
         */
        INNODB("InnoDB", "MySQL默认事务性存储引擎"),

        /**
         * Memory存储引擎（预留）
         *
         * 特点:
         * - 数据存储在内存中
         * - 极快的查询速度
         * - 重启后数据丢失
         * - 适合临时表
         */
        MEMORY("Memory", "内存存储引擎，数据存在内存中"),

        /**
         * CSV存储引擎（预留）
         *
         * 特点:
         * - 数据以CSV格式存储
         * - 可直接用Excel编辑
         * - 不支持索引
         * - 适合数据导入导出
         */
        CSV("CSV", "CSV文件存储引擎");

        private final String name;
        private final String description;

        EngineType(String name, String description) {
            this.name = name;
            this.description = description;
        }

        public String getName() {
            return name;
        }

        public String getDescription() {
            return description;
        }
    }

    /**
     * 创建存储引擎（默认配置）
     *
     * 使用默认配置创建InnoDB引擎：
     * - BufferPool大小 = bufferPoolSize
     * - 元数据持久化 = 禁用
     * - 数据目录 = "./data"
     *
     * @param engineType 引擎类型
     * @param bufferPoolSize BufferPool大小（页数）
     * @return 存储引擎实例
     * @throws IllegalArgumentException 不支持的引擎类型
     */
    public static StorageEngine createEngine(EngineType engineType, int bufferPoolSize) {
        return createEngine(engineType, bufferPoolSize, false, CommonConstant.DATA_PREFIX);
    }

    /**
     * 创建存储引擎（完整配置）
     *
     * @param engineType 引擎类型
     * @param bufferPoolSize BufferPool大小（页数）
     * @param enableMetadataPersistence 是否启用元数据持久化
     * @param dataDir 数据目录路径
     * @return 存储引擎实例
     * @throws IllegalArgumentException 不支持的引擎类型
     */
    public static StorageEngine createEngine(EngineType engineType,
                                             int bufferPoolSize,
                                             boolean enableMetadataPersistence,
                                             String dataDir) {
        switch (engineType) {
            case INNODB:
                return new InnoDBStorageEngine(bufferPoolSize, enableMetadataPersistence, dataDir);

            case MEMORY:
                throw new UnsupportedOperationException(
                        "Memory storage engine is not implemented yet"
                );

            case CSV:
                throw new UnsupportedOperationException(
                        "CSV storage engine is not implemented yet"
                );

            default:
                throw new IllegalArgumentException(
                        "Unsupported engine type: " + engineType
                );
        }
    }

    /**
     * 创建默认存储引擎
     *
     * 默认使用InnoDB引擎，标准配置。
     *
     * @return InnoDB存储引擎实例
     */
    public static StorageEngine createDefaultEngine() {
        return createEngine(EngineType.INNODB, 1024);
    }

    /**
     * 创建默认存储引擎（自定义BufferPool大小）
     *
     * @param bufferPoolSize BufferPool大小（页数）
     * @return InnoDB存储引擎实例
     */
    public static StorageEngine createDefaultEngine(int bufferPoolSize) {
        return createEngine(EngineType.INNODB, bufferPoolSize);
    }
}
