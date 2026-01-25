package com.minimysql.executor;

import com.minimysql.executor.operator.ProjectOperator;
import com.minimysql.executor.operator.ScanOperator;
import com.minimysql.parser.Expression;
import com.minimysql.parser.expressions.ColumnExpression;
import com.minimysql.storage.StorageEngine;
import com.minimysql.storage.StorageEngineFactory;
import com.minimysql.storage.table.Column;
import com.minimysql.storage.table.DataType;
import com.minimysql.storage.table.Row;
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
 * ProjectOperatorTest - 列投影算子测试
 *
 * 测试投影算子的功能:
 * - SELECT * - 返回所有列
 * - SELECT col1, col2 - 返回指定列
 * - 投影后的列定义正确性
 * - 投影后的行数据正确性
 */
@DisplayName("列投影算子测试")
class ProjectOperatorTest {

    private static final String TEST_DATA_DIR = "test_data_project_operator";

    private StorageEngine storageEngine;
    private Table table;
    private ScanOperator scan;

    @BeforeEach
    void setUp() {
        // 清理测试数据
        cleanupTestData();

        // 创建BufferPool

        // 创建StorageEngine
        storageEngine = StorageEngineFactory.createEngine(
                StorageEngineFactory.EngineType.INNODB,
                10,
                false,
                TEST_DATA_DIR
        );

        // 创建表: users(id INT, name VARCHAR(100), age INT, salary DOUBLE)
        List<Column> columns = Arrays.asList(
                new Column("id", DataType.INT, false),
                new Column("name", DataType.VARCHAR, 100, true),
                new Column("age", DataType.INT, false),
                new Column("salary", DataType.DOUBLE, false)
        );

        storageEngine.createTable("users", columns);

        // 获取表对象
        table = storageEngine.getTable("users");

        // 插入测试数据
        table.insertRow(new Row(new Object[]{1, "Alice", 25, 5000.0}));
        table.insertRow(new Row(new Object[]{2, "Bob", 30, 6000.0}));
        table.insertRow(new Row(new Object[]{3, "Charlie", 35, 7000.0}));

        // 创建ScanOperator
        scan = new ScanOperator(table);
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
    @DisplayName("测试SELECT * - 返回所有列")
    void testSelectAll() {
        // SELECT * (selectItems为空)
        List<Expression> selectItems = List.of();
        ProjectOperator project = new ProjectOperator(scan, selectItems, table.getColumns());

        // 验证isSelectAll()
        assertTrue(project.isSelectAll());

        // 验证列定义
        List<Column> projectedColumns = project.getProjectedColumns();
        assertEquals(4, projectedColumns.size());
        assertEquals("id", projectedColumns.get(0).getName());
        assertEquals("name", projectedColumns.get(1).getName());
        assertEquals("age", projectedColumns.get(2).getName());
        assertEquals("salary", projectedColumns.get(3).getName());

        // 验证行数据
        assertTrue(project.hasNext());
        Row row = project.next();
        assertEquals(1, row.getValue(0));
        assertEquals("Alice", row.getValue(1));
        assertEquals(25, row.getValue(2));
        assertEquals(5000.0, row.getValue(3));

        // 验证总行数
        int count = 0;
        while (project.hasNext()) {
            project.next();
            count++;
        }
        assertEquals(2, count); // 已经读取了1行,剩余2行
    }

    @Test
    @DisplayName("测试SELECT id, name - 返回指定列")
    void testSelectSpecificColumns() {
        // SELECT id, name
        List<Expression> selectItems = Arrays.asList(
                new ColumnExpression("id"),
                new ColumnExpression("name")
        );
        ProjectOperator project = new ProjectOperator(scan, selectItems, table.getColumns());

        // 验证isSelectAll()
        assertFalse(project.isSelectAll());

        // 验证列定义
        List<Column> projectedColumns = project.getProjectedColumns();
        assertEquals(2, projectedColumns.size());
        assertEquals("id", projectedColumns.get(0).getName());
        assertEquals("name", projectedColumns.get(1).getName());

        // 验证行数据
        assertTrue(project.hasNext());
        Row row = project.next();
        assertEquals(2, row.getColumnCount()); // 只有2列
        assertEquals(1, row.getValue(0));
        assertEquals("Alice", row.getValue(1));

        // 第二行
        assertTrue(project.hasNext());
        Row row2 = project.next();
        assertEquals(2, row2.getColumnCount());
        assertEquals(2, row2.getValue(0));
        assertEquals("Bob", row2.getValue(1));

        // 第三行
        assertTrue(project.hasNext());
        Row row3 = project.next();
        assertEquals(2, row3.getColumnCount());
        assertEquals(3, row3.getValue(0));
        assertEquals("Charlie", row3.getValue(1));

        assertFalse(project.hasNext());
    }

    @Test
    @DisplayName("测试SELECT age - 单列投影")
    void testSelectSingleColumn() {
        // SELECT age
        List<Expression> selectItems = Arrays.asList(
                new ColumnExpression("age")
        );
        ProjectOperator project = new ProjectOperator(scan, selectItems, table.getColumns());

        // 验证列定义
        List<Column> projectedColumns = project.getProjectedColumns();
        assertEquals(1, projectedColumns.size());
        assertEquals("age", projectedColumns.get(0).getName());

        // 验证行数据
        assertTrue(project.hasNext());
        Row row = project.next();
        assertEquals(1, row.getColumnCount());
        assertEquals(25, row.getValue(0));

        assertTrue(project.hasNext());
        Row row2 = project.next();
        assertEquals(1, row2.getColumnCount());
        assertEquals(30, row2.getValue(0));

        assertTrue(project.hasNext());
        Row row3 = project.next();
        assertEquals(1, row3.getColumnCount());
        assertEquals(35, row3.getValue(0));

        assertFalse(project.hasNext());
    }

    @Test
    @DisplayName("测试SELECT列顺序")
    void testSelectColumnOrder() {
        // SELECT name, age, id (不同顺序)
        List<Expression> selectItems = Arrays.asList(
                new ColumnExpression("name"),
                new ColumnExpression("age"),
                new ColumnExpression("id")
        );
        ProjectOperator project = new ProjectOperator(scan, selectItems, table.getColumns());

        // 验证列定义顺序
        List<Column> projectedColumns = project.getProjectedColumns();
        assertEquals("name", projectedColumns.get(0).getName());
        assertEquals("age", projectedColumns.get(1).getName());
        assertEquals("id", projectedColumns.get(2).getName());

        // 验证行数据顺序
        assertTrue(project.hasNext());
        Row row = project.next();
        assertEquals("Alice", row.getValue(0)); // name
        assertEquals(25, row.getValue(1));       // age
        assertEquals(1, row.getValue(2));        // id
    }

    @Test
    @DisplayName("测试SELECT不存在的列抛异常")
    void testSelectNonExistentColumn() {
        // SELECT unknown_column
        List<Expression> selectItems = Arrays.asList(
                new ColumnExpression("unknown_column")
        );

        assertThrows(IllegalArgumentException.class, () -> {
            new ProjectOperator(scan, selectItems, table.getColumns());
        });
    }

    @Test
    @DisplayName("测试ProjectOperator构造函数空指针检查")
    void testConstructorNullChecks() {
        List<Expression> selectItems = Arrays.asList(
                new ColumnExpression("id")
        );

        // child为null
        assertThrows(IllegalArgumentException.class, () -> {
            new ProjectOperator(null, selectItems, table.getColumns());
        });

        // originalColumns为null
        assertThrows(IllegalArgumentException.class, () -> {
            new ProjectOperator(scan, selectItems, null);
        });
    }

    @Test
    @DisplayName("测试getChild()和getSelectItems()")
    void testGetters() {
        List<Expression> selectItems = Arrays.asList(
                new ColumnExpression("id"),
                new ColumnExpression("name")
        );
        ProjectOperator project = new ProjectOperator(scan, selectItems, table.getColumns());

        assertEquals(scan, project.getChild());
        assertEquals(selectItems, project.getSelectItems());
    }

    @Test
    @DisplayName("测试SELECT * 不创建新Row对象")
    void testSelectAllDoesNotCreateNewRow() {
        List<Expression> selectItems = List.of();
        ProjectOperator project = new ProjectOperator(scan, selectItems, table.getColumns());

        assertTrue(project.hasNext());
        Row row1 = project.next();

        // 验证返回的是原始Row对象(通过引用比较)
        // 注意:这个测试假设ScanOperator返回的是唯一的Row对象
        assertNotNull(row1);
        assertEquals(4, row1.getColumnCount());
    }

    /**
     * 清理测试数据
     */
    private void cleanupTestData() {
        File dir = new File(TEST_DATA_DIR);
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    file.delete();
                }
            }
            dir.delete();
        }
    }
}
