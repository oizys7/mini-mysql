package com.minimysql.parser;

/**
 * Statement - SQL语句接口
 *
 * 所有SQL语句的基类,代表一个完整的、可执行的SQL命令。
 *
 * 设计原则:
 * - "Good taste": 所有语句都是Statement,消除if-else判断SQL类型
 * - 统一接口: Executor只需要调用execute(),不需要关心具体SQL类型
 * - 类型安全: 每种SQL有专门的子类,编译期检查
 *
 * 使用示例:
 * <pre>
 * Statement stmt = parser.parse("CREATE TABLE users (id INT, name VARCHAR(100))");
 * if (stmt instanceof CreateTableStatement) {
 *     CreateTableStatement create = (CreateTableStatement) stmt;
 *     String tableName = create.getTableName();
 *     ...
 * }
 * </pre>
 *
 * "Never break userspace": 添加新的SQL类型不影响现有代码
 */
public interface Statement {

    /**
     * 获取语句类型
     *
     * @return 语句类型枚举
     */
    StatementType getType();

    /**
     * SQL语句类型枚举
     *
     * 定义所有支持的SQL语句类型。
     */
    enum StatementType {
        /** CREATE TABLE - 创建表 */
        CREATE_TABLE,
        /** DROP TABLE - 删除表 */
        DROP_TABLE,
        /** SELECT - 查询 */
        SELECT,
        /** INSERT - 插入 */
        INSERT,
        /** UPDATE - 更新 */
        UPDATE,
        /** DELETE - 删除 */
        DELETE
    }
}
