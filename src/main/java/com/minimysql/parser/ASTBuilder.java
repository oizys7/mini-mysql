package com.minimysql.parser;

import com.minimysql.parser.expressions.*;
import com.minimysql.parser.statements.*;
import com.minimysql.storage.table.DataType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ASTBuilder - 将ANTLR AST转换为Statement对象
 *
 * 使用访问者模式遍历ANTLR生成的语法树,将其转换为强类型的Statement对象。
 *
 * 设计原则:
 * - "Good taste": 每个visit方法只做一件事,清晰明了
 * - 消除特殊情况: 统一处理所有表达式
 * - 类型安全: 将ANTLR的弱类型AST转换为强类型对象
 *
 * 错误处理:
 * - 如果遇到不支持的语法,抛出ParseException
 * - 提供清晰的错误信息,指出SQL中的问题位置
 */
public class ASTBuilder extends MySQLBaseVisitor<Object> {

    @Override
    public Object visitSqlStatement(MySQLParser.SqlStatementContext ctx) {
        // 顶层规则,直接委托给具体的语句
        return visitChildren(ctx);
    }

    // ==================== CREATE TABLE ====================

    @Override
    public CreateTableStatement visitCreateTableStatement(MySQLParser.CreateTableStatementContext ctx) {
        String tableName = visitIdentifier(ctx.tableName);

        // 解析列定义
        List<ColumnDefinition> columns = new ArrayList<>();
        for (MySQLParser.ColumnDefinitionContext colCtx : ctx.columnDefinition()) {
            columns.add(visitColumnDefinition(colCtx));
        }

        // 解析主键(如果有)
        String primaryKeyColumn = null;
        if (ctx.PRIMARY() != null && ctx.KEY() != null && ctx.columnName != null) {
            primaryKeyColumn = visitIdentifier(ctx.columnName);
        }

        return new CreateTableStatement(tableName, columns, primaryKeyColumn);
    }

    @Override
    public ColumnDefinition visitColumnDefinition(MySQLParser.ColumnDefinitionContext ctx) {
        String columnName = visitIdentifier(ctx.columnName);
        DataTypeAndLength dataTypeAndLength = visitDataType(ctx.dataType());
        boolean nullable = ctx.NOT() == null; // 如果没有NOT,则允许NULL

        return new ColumnDefinition(columnName, dataTypeAndLength.dataType, dataTypeAndLength.length, nullable);
    }

    @Override
    public DataTypeAndLength visitDataType(MySQLParser.DataTypeContext ctx) {
        DataType dataType;
        int length = 0;

        if (ctx.INT() != null) {
            dataType = DataType.INT;
        } else if (ctx.BIGINT() != null) {
            dataType = DataType.BIGINT;
        } else if (ctx.DOUBLE() != null) {
            dataType = DataType.DOUBLE;
        } else if (ctx.BOOLEAN() != null) {
            dataType = DataType.BOOLEAN;
        } else if (ctx.VARCHAR() != null) {
            dataType = DataType.VARCHAR;
            length = Integer.parseInt(ctx.length.getText());
        } else if (ctx.DATE() != null) {
            dataType = DataType.DATE;
        } else if (ctx.TIMESTAMP() != null) {
            dataType = DataType.TIMESTAMP;
        } else {
            throw new ParseException("Unsupported data type: " + ctx.getText());
        }

        return new DataTypeAndLength(dataType, length);
    }

    // ==================== DROP TABLE ====================

    @Override
    public DropTableStatement visitDropTableStatement(MySQLParser.DropTableStatementContext ctx) {
        String tableName = visitIdentifier(ctx.tableName);
        return new DropTableStatement(tableName);
    }

    // ==================== SELECT ====================

    @Override
    public SelectStatement visitSelectStatement(MySQLParser.SelectStatementContext ctx) {
        // 解析SELECT列表
        List<Expression> selectItems = new ArrayList<>();
        if (ctx.selectItems().STAR() != null) {
            // SELECT *, 空列表表示*
            selectItems = List.of();
        } else {
            for (MySQLParser.ExpressionContext exprCtx : ctx.selectItems().expression()) {
                selectItems.add((Expression) visit(exprCtx));
            }
        }

        // 解析表名
        String tableName = visitIdentifier(ctx.tableName);

        // 解析WHERE条件(如果有)
        Expression whereClause = null;
        if (ctx.whereExpr != null) {
            whereClause = (Expression) visit(ctx.whereExpr);
        }

        return new SelectStatement(selectItems, tableName, whereClause);
    }

    // ==================== INSERT ====================

    @Override
    public InsertStatement visitInsertStatement(MySQLParser.InsertStatementContext ctx) {
        String tableName = visitIdentifier(ctx.tableName);

        // 解析列名(如果有)
        // ctx.identifier() 包含 tableName 和可能的列名列表
        // 如果有列名列表,则 identifier 列表大小 > 1(第一个是tableName)
        List<String> columnNames = new ArrayList<>();
        List<MySQLParser.IdentifierContext> allIdents = ctx.identifier();

        // 从第二个标识符开始才是列名(第一个是tableName)
        if (allIdents.size() > 1) {
            for (int i = 1; i < allIdents.size(); i++) {
                columnNames.add(allIdents.get(i).getText());
            }
        }

        // 解析值列表(支持多行插入)
        List<List<Expression>> values = new ArrayList<>();
        for (MySQLParser.InsertStatementValueRowContext rowCtx : ctx.insertStatementValueRow()) {
            List<Expression> rowValues = new ArrayList<>();
            for (MySQLParser.ExpressionContext exprCtx : rowCtx.expression()) {
                rowValues.add((Expression) visit(exprCtx));
            }
            values.add(rowValues);
        }

        return new InsertStatement(tableName, columnNames, values);
    }

    // ==================== UPDATE ====================

    @Override
    public UpdateStatement visitUpdateStatement(MySQLParser.UpdateStatementContext ctx) {
        String tableName = visitIdentifier(ctx.tableName);

        // 解析SET子句
        Map<String, Expression> assignments = new HashMap<>();
        for (MySQLParser.SetItemContext setItemCtx : ctx.setItem()) {
            String columnName = visitIdentifier(setItemCtx.columnName);
            Expression valueExpr = (Expression) visit(setItemCtx.valueExpr);
            assignments.put(columnName, valueExpr);
        }

        // 解析WHERE条件(如果有)
        Expression whereClause = null;
        if (ctx.whereExpr != null) {
            whereClause = (Expression) visit(ctx.whereExpr);
        }

        return new UpdateStatement(tableName, assignments, whereClause);
    }

    // ==================== DELETE ====================

    @Override
    public DeleteStatement visitDeleteStatement(MySQLParser.DeleteStatementContext ctx) {
        String tableName = visitIdentifier(ctx.tableName);

        // 解析WHERE条件(如果有)
        Expression whereClause = null;
        if (ctx.whereExpr != null) {
            whereClause = (Expression) visit(ctx.whereExpr);
        }

        return new DeleteStatement(tableName, whereClause);
    }

    // ==================== 表达式 ====================

    @Override
    public Expression visitParenthesisExpr(MySQLParser.ParenthesisExprContext ctx) {
        return (Expression) visit(ctx.expression());
    }

    @Override
    public Expression visitArithmeticExpr(MySQLParser.ArithmeticExprContext ctx) {
        Expression left = (Expression) visit(ctx.expression(0));
        Expression right = (Expression) visit(ctx.expression(1));
        OperatorEnum operator = OperatorEnum.fromSymbol(ctx.op.getText());

        if (operator == null) {
            throw new ParseException("Unknown operator: " + ctx.op.getText());
        }

        return new BinaryExpression(left, operator, right);
    }

    @Override
    public Expression visitComparisonExpr(MySQLParser.ComparisonExprContext ctx) {
        Expression left = (Expression) visit(ctx.expression(0));
        Expression right = (Expression) visit(ctx.expression(1));
        OperatorEnum operator = OperatorEnum.fromSymbol(ctx.op.getText());

        if (operator == null) {
            throw new ParseException("Unknown operator: " + ctx.op.getText());
        }

        return new BinaryExpression(left, operator, right);
    }

    @Override
    public Expression visitNotExpr(MySQLParser.NotExprContext ctx) {
        Expression operand = (Expression) visit(ctx.expression());
        return new NotExpression(operand);
    }

    @Override
    public Expression visitLogicalExpr(MySQLParser.LogicalExprContext ctx) {
        Expression left = (Expression) visit(ctx.expression(0));
        Expression right = (Expression) visit(ctx.expression(1));
        OperatorEnum operator = OperatorEnum.fromSymbol(ctx.op.getText());

        if (operator == null) {
            throw new ParseException("Unknown operator: " + ctx.op.getText());
        }

        return new BinaryExpression(left, operator, right);
    }

    @Override
    public Expression visitLiteralExpr(MySQLParser.LiteralExprContext ctx) {
        return (Expression) visit(ctx.literal());
    }

    @Override
    public Expression visitColumnExpr(MySQLParser.ColumnExprContext ctx) {
        // 解析列名(可能包含表名前缀,如users.id)
        List<String> parts = new ArrayList<>();
        for (MySQLParser.IdentifierContext identCtx : ctx.identifier()) {
            parts.add(identCtx.getText());
        }
        return new ColumnExpression(parts);
    }

    // ==================== 字面量 ====================

    @Override
    public Object visitLiteral(MySQLParser.LiteralContext ctx) {
        if (ctx.INTEGER_LITERAL() != null) {
            return new LiteralExpression(Integer.parseInt(ctx.getText()));
        } else if (ctx.DECIMAL_LITERAL() != null) {
            return new LiteralExpression(Double.parseDouble(ctx.getText()));
        } else if (ctx.STRING_LITERAL() != null) {
            // 去除单引号,处理双单引号转义
            String text = ctx.getText();
            text = text.substring(1, text.length() - 1); // 去除首尾单引号
            text = text.replace("''", "'"); // 处理转义
            return new LiteralExpression(text);
        } else if (ctx.BOOLEAN_LITERAL() != null) {
            boolean value = ctx.getText().equals("TRUE");
            return new LiteralExpression(value);
        } else if (ctx.NULL_() != null) {
            return new LiteralExpression(null);
        } else {
            throw new ParseException("Unknown literal: " + ctx.getText());
        }
    }

    // ==================== 标识符 ====================

    @Override
    public String visitIdentifier(MySQLParser.IdentifierContext ctx) {
        return ctx.getText();
    }

    // ==================== 辅助类 ====================

    /**
     * 数据类型和长度的组合
     */
    private static class DataTypeAndLength {
        final DataType dataType;
        final int length;

        DataTypeAndLength(DataType dataType, int length) {
            this.dataType = dataType;
            this.length = length;
        }
    }
}
