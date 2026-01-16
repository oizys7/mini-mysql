package com.minimysql.metadata;

import com.minimysql.storage.StorageEngine;
import com.minimysql.storage.impl.InnoDBStorageEngine;
import com.minimysql.storage.table.Column;
import com.minimysql.storage.table.DataType;
import com.minimysql.storage.table.Row;
import com.minimysql.storage.table.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 元数据管理演示测试
 *
 * 展示元数据管理的完整流程。
 */
public class MetadataDemoTest {

    private StorageEngine storageEngine;

    @BeforeEach
    public void setUp() {
        storageEngine = new InnoDBStorageEngine(100, true);
    }

    @AfterEach
    public void tearDown() {
        if (storageEngine != null) {
            storageEngine.close();
        }
    }

    @Test
    @DisplayName("元数据管理完整流程")
    public void testMetadataManagementWorkflow() {
        // 1. 验证系统表已自动创建
        assertTrue(storageEngine.tableExists(SystemTables.SYS_TABLES));
        assertTrue(storageEngine.tableExists(SystemTables.SYS_COLUMNS));

        // 2. 创建业务表
        List<Column> userColumns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("username", DataType.VARCHAR, 50, false),
                new Column("email", DataType.VARCHAR, 100, true),
                new Column("age", DataType.INT, false)
        );

        Table usersTable = storageEngine.createTable("users", userColumns);

        assertEquals("users", usersTable.getTableName());
        assertTrue(usersTable.getTableId() > 0);
        assertEquals(4, usersTable.getColumnCount());

        // 3. 插入数据
        Object[] user1 = {1, "alice", "alice@example.com", 25};
        Object[] user2 = {2, "bob", "bob@example.com", 30};

        usersTable.insertRow(new Row(userColumns, user1));
        usersTable.insertRow(new Row(userColumns, user2));

        // 4. 查询数据
        Row result = usersTable.selectByPrimaryKey(1);
        assertNotNull(result);
        assertEquals(1, result.getValue(0));
        assertEquals("alice", result.getValue(1));
        assertEquals("alice@example.com", result.getValue(2));
        assertEquals(25, result.getValue(3));

        // 5. 创建另一个表
        List<Column> postColumns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("title", DataType.VARCHAR, 200, false),
                new Column("user_id", DataType.INT, false)
        );

        Table postsTable = storageEngine.createTable("posts", postColumns);

        assertEquals("posts", postsTable.getTableName());
        assertNotEquals(usersTable.getTableId(), postsTable.getTableId());

        // 6. 列出所有表(包括系统表)
        List<String> tableNames = storageEngine.getAllTableNames();
        assertTrue(tableNames.contains("users"));
        assertTrue(tableNames.contains("posts"));
        assertTrue(tableNames.contains(SystemTables.SYS_TABLES));
        assertTrue(tableNames.contains(SystemTables.SYS_COLUMNS));

        // 7. 验证系统表受保护
        assertThrows(IllegalArgumentException.class, () -> {
            storageEngine.dropTable(SystemTables.SYS_TABLES);
        });

        assertThrows(IllegalArgumentException.class, () -> {
            storageEngine.createTable("SYS_TABLES", Arrays.asList(new Column("id", DataType.INT, false)));
        });

        // 8. 删除业务表(跳过，因为B+树删除未实现)
        // assertTrue(storageEngine.dropTable("users"));
        // assertFalse(storageEngine.tableExists("users"));
    }
}
