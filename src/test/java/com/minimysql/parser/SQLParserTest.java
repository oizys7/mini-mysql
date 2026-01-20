package com.minimysql.parser;

import com.minimysql.parser.expressions.*;
import com.minimysql.parser.statements.*;
import com.minimysql.storage.table.DataType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SQLParserTest - SQL解析器单元测试
 *
 * 测试各种SQL语句的解析功能。
 *
 * 设计原则:
 * - 每个测试用例只测试一种SQL类型
 * - 测试正常情况,不测试错误情况(错误由ANTLR处理)
 * - "Good taste": 测试代码清晰易懂,没有复杂的setup
 */
@DisplayName("SQL 解析器测试")
class SQLParserTest {

    private final SQLParser parser = new SQLParser();

    @Test
    @DisplayName("解析 CREATE TABLE 语句 - 基本表")
    void testCreateTableBasic() {
        String sql = "CREATE TABLE users (id INT NOT NULL, name VARCHAR(100));";

        Statement stmt = parser.parse(sql);

        assertInstanceOf(CreateTableStatement.class, stmt);
        CreateTableStatement create = (CreateTableStatement) stmt;

        assertEquals("users", create.getTableName());
        assertEquals(2, create.getColumns().size());

        ColumnDefinition idCol = create.getColumns().get(0);
        assertEquals("id", idCol.getColumnName());
        assertEquals(DataType.INT, idCol.getDataType());
        assertFalse(idCol.isNullable());

        ColumnDefinition nameCol = create.getColumns().get(1);
        assertEquals("name", nameCol.getColumnName());
        assertEquals(DataType.VARCHAR, nameCol.getDataType());
        assertEquals(100, nameCol.getLength());
        assertTrue(nameCol.isNullable());
    }

    @Test
    @DisplayName("解析 CREATE TABLE 语句 - 带主键")
    void testCreateTableWithPrimaryKey() {
        String sql = "CREATE TABLE users (id INT NOT NULL, name VARCHAR(100), PRIMARY KEY (id));";

        Statement stmt = parser.parse(sql);

        assertInstanceOf(CreateTableStatement.class, stmt);
        CreateTableStatement create = (CreateTableStatement) stmt;

        assertTrue(create.getPrimaryKeyColumn().isPresent());
        assertEquals("id", create.getPrimaryKeyColumn().get());
    }

    @Test
    @DisplayName("解析 DROP TABLE 语句")
    void testDropTable() {
        String sql = "DROP TABLE users;";

        Statement stmt = parser.parse(sql);

        assertInstanceOf(DropTableStatement.class, stmt);
        DropTableStatement drop = (DropTableStatement) stmt;

        assertEquals("users", drop.getTableName());
    }

    @Test
    @DisplayName("解析 SELECT * 语句")
    void testSelectAll() {
        String sql = "SELECT * FROM users;";

        Statement stmt = parser.parse(sql);

        assertInstanceOf(SelectStatement.class, stmt);
        SelectStatement select = (SelectStatement) stmt;

        assertTrue(select.isSelectAll());
        assertEquals("users", select.getTableName());
        assertTrue(select.getWhereClause().isEmpty());
    }

    @Test
    @DisplayName("解析 SELECT 列表 语句")
    void testSelectColumns() {
        String sql = "SELECT id, name, age FROM users;";

        Statement stmt = parser.parse(sql);

        assertInstanceOf(SelectStatement.class, stmt);
        SelectStatement select = (SelectStatement) stmt;

        assertFalse(select.isSelectAll());
        assertEquals(3, select.getSelectItems().size());
        assertEquals("id", ((ColumnExpression) select.getSelectItems().get(0)).getColumnName());
    }

    @Test
    @DisplayName("解析 SELECT WHERE 语句")
    void testSelectWithWhere() {
        String sql = "SELECT * FROM users WHERE age > 18;";

        Statement stmt = parser.parse(sql);

        assertInstanceOf(SelectStatement.class, stmt);
        SelectStatement select = (SelectStatement) stmt;

        assertTrue(select.getWhereClause().isPresent());

        Expression where = select.getWhereClause().get();
        assertInstanceOf(BinaryExpression.class, where);

        BinaryExpression binary = (BinaryExpression) where;
        assertEquals(Operator.GREATER_THAN, binary.getOperator());
    }

    @Test
    @DisplayName("解析 INSERT 语句 - 单行")
    void testInsertSingleRow() {
        String sql = "INSERT INTO users VALUES (1, 'Alice', 25);";

        Statement stmt = parser.parse(sql);

        assertInstanceOf(InsertStatement.class, stmt);
        InsertStatement insert = (InsertStatement) stmt;

        assertEquals("users", insert.getTableName());
        assertFalse(insert.hasColumnNames());
        assertEquals(1, insert.getRowCount());
        assertEquals(3, insert.getValues().get(0).size());
    }

    @Test
    @DisplayName("解析 INSERT 语句 - 带列名")
    void testInsertWithColumns() {
        String sql = "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);";

        Statement stmt = parser.parse(sql);

        assertInstanceOf(InsertStatement.class, stmt);
        InsertStatement insert = (InsertStatement) stmt;

        assertTrue(insert.hasColumnNames());
        assertEquals(3, insert.getColumnNames().size());
        assertEquals("id", insert.getColumnNames().get(0));
    }

    @Test
    @DisplayName("解析 INSERT 语句 - 多行")
    void testInsertMultipleRows() {
        String sql = "INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30);";

        Statement stmt = parser.parse(sql);

        assertInstanceOf(InsertStatement.class, stmt);
        InsertStatement insert = (InsertStatement) stmt;

        assertEquals(2, insert.getRowCount());
    }

    @Test
    @DisplayName("解析 UPDATE 语句")
    void testUpdate() {
        String sql = "UPDATE users SET age = 26 WHERE id = 1;";

        Statement stmt = parser.parse(sql);

        assertInstanceOf(UpdateStatement.class, stmt);
        UpdateStatement update = (UpdateStatement) stmt;

        assertEquals("users", update.getTableName());
        assertEquals(1, update.getAssignments().size());
        assertTrue(update.getAssignments().containsKey("age"));
        assertTrue(update.getWhereClause().isPresent());
    }

    @Test
    @DisplayName("解析 DELETE 语句")
    void testDelete() {
        String sql = "DELETE FROM users WHERE id = 1;";

        Statement stmt = parser.parse(sql);

        assertInstanceOf(DeleteStatement.class, stmt);
        DeleteStatement delete = (DeleteStatement) stmt;

        assertEquals("users", delete.getTableName());
        assertTrue(delete.getWhereClause().isPresent());
    }

    @Test
    @DisplayName("解析复杂 WHERE 表达式")
    void testComplexWhereExpression() {
        String sql = "SELECT * FROM users WHERE age > 18 AND age < 65;";

        Statement stmt = parser.parse(sql);

        assertInstanceOf(SelectStatement.class, stmt);
        SelectStatement select = (SelectStatement) stmt;

        Expression where = select.getWhereClause().get();
        assertInstanceOf(BinaryExpression.class, where);

        BinaryExpression andExpr = (BinaryExpression) where;
        assertEquals(Operator.AND, andExpr.getOperator());
    }

    @Test
    @DisplayName("解析不带分号的SQL语句")
    void testSQLWithoutSemicolon() {
        String sql = "SELECT * FROM users";

        Statement stmt = parser.parse(sql);

        assertInstanceOf(SelectStatement.class, stmt);
    }

    @Test
    @DisplayName("错误处理 - 空SQL")
    void testEmptySQL() {
        ParseException exception = assertThrows(ParseException.class, () -> {
            parser.parse("");
        });

        assertTrue(exception.getMessage().contains("null or empty"));
    }

    @Test
    @DisplayName("错误处理 - 语法错误")
    void testSyntaxError() {
        ParseException exception = assertThrows(ParseException.class, () -> {
            parser.parse("CREAT TABLE users (id INT);"); // 拼写错误: CREAT
        });

        assertTrue(exception.getMessage().contains("Syntax error"));
    }
}
