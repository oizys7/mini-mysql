package com.minimysql.parser;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ConsoleErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

import java.util.ArrayList;
import java.util.List;

/**
 * SQLParser - SQL解析器对外接口
 *
 * 提供简单易用的API来解析SQL字符串。
 *
 * 设计原则:
 * - 简单的接口: 一个方法 parse(String sql)
 * - 清晰的错误信息: 语法错误时提供位置和原因
 * - 实用主义: 不需要复杂的配置,开箱即用
 *
 * 使用示例:
 * <pre>
 * SQLParser parser = new SQLParser();
 * Statement stmt = parser.parse("CREATE TABLE users (id INT, name VARCHAR(100))");
 *
 * if (stmt instanceof CreateTableStatement) {
 *     CreateTableStatement create = (CreateTableStatement) stmt;
 *     System.out.println("Table: " + create.getTableName());
 * }
 * </pre>
 *
 * "Good taste": 错误处理直接暴露,而不是被掩盖
 */
public class SQLParser {

    /**
     * 解析SQL字符串
     *
     * @param sql SQL语句
     * @return 解析后的Statement对象
     * @throws ParseException 如果SQL语法错误
     */
    public Statement parse(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            throw new ParseException("SQL statement cannot be null or empty");
        }

        try {
            // 1. 词法分析
            MySQLExceptionCollector errorCollector = new MySQLExceptionCollector();
            MySQLLexer lexer = new MySQLLexer(CharStreams.fromString(sql));
            lexer.removeErrorListeners();
            lexer.addErrorListener(errorCollector);

            // 2. 语法分析
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            MySQLParser parser = new MySQLParser(tokens);
            parser.removeErrorListeners();
            parser.addErrorListener(errorCollector);

            // 3. 解析并构建AST
            MySQLParser.SqlStatementContext tree = parser.sqlStatement();

            // 检查是否有错误
            if (errorCollector.hasErrors()) {
                throw new ParseException(errorCollector.getErrorMessage());
            }

            // 4. 将ANTLR AST转换为Statement对象
            ASTBuilder builder = new ASTBuilder();
            Statement statement = (Statement) builder.visit(tree);

            return statement;

        } catch (ParseException e) {
            throw e;
        } catch (Exception e) {
            throw new ParseException("Failed to parse SQL: " + e.getMessage(), e);
        }
    }

    /**
     * ANTLR错误收集器
     *
     * 收集词法和语法错误,提供清晰的错误信息。
     */
    private static class MySQLExceptionCollector extends ConsoleErrorListener {
        private final List<String> errors = new ArrayList<>();

        @Override
        public void syntaxError(Recognizer<?, ?> recognizer,
                               Object offendingSymbol,
                               int line,
                               int charPositionInLine,
                               String msg,
                               RecognitionException e) {
            String error = String.format("Syntax error at line %d:%d - %s",
                                       line, charPositionInLine, msg);
            errors.add(error);
        }

        public boolean hasErrors() {
            return !errors.isEmpty();
        }

        public String getErrorMessage() {
            return String.join("\n", errors);
        }
    }

    /**
     * 测试入口
     */
    public static void main(String[] args) {
        SQLParser parser = new SQLParser();

        // 测试各种SQL语句
        String[] testCases = {
            "CREATE TABLE users (id INT NOT NULL, name VARCHAR(100), PRIMARY KEY (id));",
            "SELECT * FROM users;",
            "SELECT id, name FROM users WHERE age > 18;",
            "INSERT INTO users VALUES (1, 'Alice', 25);",
            "UPDATE users SET age = 26 WHERE id = 1;",
            "DELETE FROM users WHERE id = 1;",
            "DROP TABLE users;"
        };

        for (String sql : testCases) {
            System.out.println("Parsing: " + sql);
            try {
                Statement stmt = parser.parse(sql);
                System.out.println("✓ Success: " + stmt.getType());
                System.out.println("  " + stmt);
            } catch (ParseException e) {
                System.out.println("✗ Error: " + e.getMessage());
            }
            System.out.println();
        }
    }
}
