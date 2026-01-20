package com.minimysql.parser.statements;

import com.minimysql.parser.Expression;
import com.minimysql.parser.Statement;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * UpdateStatement - UPDATE更新语句
 *
 * 表示更新数据的SQL语句。
 *
 * 语法示例:
 * <pre>
 * UPDATE users SET age = 26 WHERE id = 1;
 * UPDATE users SET age = 26, name = 'Alice' WHERE id = 1;
 * </pre>
 *
 * 设计原则:
 * - 支持多列更新
 * - 支持WHERE条件(可选,但没有WHERE会更新所有行!)
 */
public class UpdateStatement implements Statement {

    /** 表名 */
    private final String tableName;

    /** 更新映射(列名 -> 新值表达式) */
    private final Map<String, Expression> assignments;

    /** WHERE条件(如果没有WHERE子句则为空) */
    private final Expression whereClause;

    public UpdateStatement(String tableName,
                          Map<String, Expression> assignments,
                          Expression whereClause) {
        this.tableName = tableName;
        this.assignments = Map.copyOf(assignments);
        this.whereClause = whereClause;
    }

    public String getTableName() {
        return tableName;
    }

    public Map<String, Expression> getAssignments() {
        return assignments;
    }

    public Optional<Expression> getWhereClause() {
        return Optional.ofNullable(whereClause);
    }

    @Override
    public StatementType getType() {
        return StatementType.UPDATE;
    }

    @Override
    public String toString() {
        return "UpdateStatement{" +
                "tableName='" + tableName + '\'' +
                ", assignments=" + assignments +
                ", whereClause=" + whereClause +
                '}';
    }
}
