package com.minimysql.metadata;

import com.minimysql.storage.StorageEngine;
import com.minimysql.storage.impl.InnoDBStorageEngine;
import com.minimysql.storage.table.Column;
import com.minimysql.storage.table.DataType;
import com.minimysql.storage.table.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MetadataLoadingTest - 元数据加载测试
 *
 * 测试SchemaManager的元数据加载功能:
 * - loadAllTables() 能从系统表加载表定义
 * - 重启后TableMetadata能被正确加载
 */
@DisplayName("元数据加载测试")
class MetadataLoadingTest {

    private static final String TEST_DATA_DIR = "test_data_metadata_loading";

    private StorageEngine storageEngine;
    private SchemaManager schemaManager;

    @BeforeEach
    void setUp() {
        // 清理测试数据
        cleanupTestData();

        // 创建StorageEngine (启用元数据持久化)
        storageEngine = new InnoDBStorageEngine(10, true);
        schemaManager = new SchemaManager(storageEngine);
    }

    @AfterEach
    void tearDown() {
        // 关闭StorageEngine
        if (storageEngine != null) {
            storageEngine.close();
        }

        // 清理测试数据
        cleanupTestData();
    }

    @Test
    @DisplayName("测试loadAllTables()能重建Table对象")
    void testLoadAllTablesRecreatesTableObjects() throws Exception {
        // 第一步：创建表
        schemaManager.initialize();

        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("name", DataType.VARCHAR, 100, true),
                new Column("age", DataType.INT, false)
        );

        int tableId = schemaManager.createTable("users", columns);

        // 第二步：验证TableMetadata已缓存
        TableMetadata metadata = schemaManager.loadTableMetadata("users");
        assertNotNull(metadata);
        assertEquals("users", metadata.getTableName());
        assertEquals(3, metadata.getColumns().size());

        // 第三步：调用loadAllTables()重建Table对象
        schemaManager.loadAllTables();

        // 第四步：验证Table对象已在StorageEngine中
        Table table = storageEngine.getTable("users");
        assertNotNull(table, "Table should be loaded and registered");
        assertEquals("users", table.getTableName());
        assertEquals(3, table.getColumnCount());

        // 第五步：验证Table已打开
        // (可以通过调用table的方法来验证，比如fullTableScan)
        // 这里我们只验证表对象存在即可
    }

    @Test
    @DisplayName("测试元数据在重启后被正确加载")
    void testMetadataLoadedAfterRestart() throws Exception {
        // 第一轮：创建表并写入元数据
        schemaManager.initialize();

        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("name", DataType.VARCHAR, 100, true)
        );

        schemaManager.createTable("products", columns);

        // 关闭
        storageEngine.close();

        // 第二轮：重新创建StorageEngine和SchemaManager
        StorageEngine newStorageEngine = new InnoDBStorageEngine(10, true);
        SchemaManager newSchemaManager = new SchemaManager(newStorageEngine);

        try {
            // 初始化(会自动加载元数据)
            newSchemaManager.initialize();

            // 验证TableMetadata被加载
            TableMetadata metadata = newSchemaManager.loadTableMetadata("products");
            assertNotNull(metadata, "TableMetadata should be loaded from system tables");
            assertEquals("products", metadata.getTableName());

            // 验证列定义正确
            assertEquals(2, metadata.getColumns().size());

        } finally {
            newStorageEngine.close();
        }
    }

    @Test
    @DisplayName("测试系统表不在元数据中")
    void testSystemTablesNotInMetadata() throws Exception {
        schemaManager.initialize();

        // 系统表不应该在metadataCache中
        assertNull(schemaManager.loadTableMetadata("SYS_TABLES"));
        assertNull(schemaManager.loadTableMetadata("SYS_COLUMNS"));
    }

    /**
     * 清理测试数据
     */
    private void cleanupTestData() {
        File dir = new File(TEST_DATA_DIR);
        if (dir.exists()) {
            deleteDirectory(dir);
        }
    }

    /**
     * 递归删除目录
     */
    private void deleteDirectory(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        dir.delete();
    }
}
