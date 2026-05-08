# 代码审查报告：Executor / Parser / Result 模块

**审查日期**: 2026-05-08
**审查范围**: `executor/`, `parser/`, `result/`
**审查人视角**: Linus Torvalds — 数据结构优先、消除特殊情况、实用主义

---

## 📋 修复记录

### 2026-05-08

#### ✅ Parser模块 - AND/OR优先级修复 (P1)
**问题**: Grammar中AND和OR写在同一规则，优先级相同，违反SQL标准
**方案**: 使用分层规则处理运算符优先级
**文件修改**:
- `src/main/antlr/com/minimysql/parser/MySQL.g4` - 重构为分层规则
- `src/main/java/com/minimysql/parser/ASTBuilder.java` - 更新visitor方法
**测试**: 所有Parser测试通过 ✅

#### ✅ Executor模块 - ScanOperator惰性加载 (P2)
**问题**: ScanOperator构造函数中立即执行全表扫描，违反火山模型
**方案**: 实现惰性迭代器链（BPlusTree → ClusteredIndex → Table → ScanOperator）
**文件修改**:
- `src/main/java/com/minimysql/storage/index/BPlusTree.java` - 添加getAllLazy()
- `src/main/java/com/minimysql/storage/index/ClusteredIndex.java` - 添加getAllRowsLazy()
- `src/main/java/com/minimysql/storage/table/Table.java` - 添加fullTableScanLazy()
- `src/main/java/com/minimysql/executor/operator/ScanOperator.java` - 使用惰性迭代器
**测试**: 所有storage和metadata测试通过 ✅

---

## 总览

| 模块 | 文件数 | 品味评分 | 质量评分 | 关键问题数 |
|------|--------|----------|----------|------------|
| executor | 12 | 🟡 凑合 | 3.5/5 | 5 |
| parser | 13 | 🟢 好品味 | 4.0/5 | 3 |
| result | 1 | 🟢 好品味 | 4.5/5 | 1 |

---

## 一、Executor 模块

### 1.1 架构评价

火山模型的实现思路正确——Operator 接口用 `hasNext()/next()` 迭代器模式，算子可自由组合。这是数据库执行引擎的经典设计，没什么好说的。

**但问题是：数据结构用错了。**

### 1.2 致命问题

#### 🔴 P1：DRY 违反 — 列名查找逻辑重复 6 次

**位置**: `ExpressionEvaluator:139-161`, `FilterOperator:121`, `ProjectOperator:212-218`, `InsertOperator:239,269`, `UpdateOperator:279,302`

```java
// 这段逻辑在 6 个文件里几乎一模一样：
for (int i = 0; i < columns.size(); i++) {
    if (columns.get(i).getName().equalsIgnoreCase(columnName)) {
        return i;
    }
}
```

**影响**: 维护噩梦。改一处忘了改另一处就是 bug。
**修复**: 提取到 Column 工具类或 Row 的 `indexOf(columnName)` 方法。

```java
// 应该这样：
public static int findColumnIndex(List<Column> columns, String name) {
    for (int i = 0; i < columns.size(); i++) {
        if (columns.get(i).getName().equalsIgnoreCase(name)) {
            return i;
        }
    }
    return -1;
}
```

#### 🔴 P2：ScanOperator 在构造函数里做全表扫描 ✅ 已修复 (2026-05-08)

**位置**: `ScanOperator:76`

```java
public ScanOperator(Table table) {
    this.table = table;
    this.rowIterator = table.fullTableScan().iterator(); // 构造时就扫描！
}
```

**影响**: 违背了火山模型"拉取"的本质。构造 Operator 不应该有副作用，更不应该把全表数据加载到内存。

**修复方案**: 实现惰性迭代器链
```java
// BPlusTree 中添加惰性迭代器
public Iterator<Object> getAllLazy() {
    // 返回按需遍历B+树叶子节点的迭代器
}

// ClusteredIndex 中添加惰性迭代器
public Iterator<Row> getAllRowsLazy() {
    // 按需反序列化物理记录为逻辑行
}

// Table 中添加惰性迭代器
public Iterator<Row> fullTableScanLazy() {
    return clusteredIndex.getAllRowsLazy();
}

// ScanOperator 使用惰性迭代器
public ScanOperator(Table table) {
    this.table = table;
    this.rowIterator = table.fullTableScanLazy(); // 惰性加载！
}
```
`hasNext()` 时才取下一行，不一次性加载所有数据到内存。

#### 🔴 P3：ExpressionEvaluator.evalBinary() 过于臃肿

**位置**: `ExpressionEvaluator:185-221`

60+ 行的方法里塞满了 switch-case，算术、比较、逻辑混在一起。

```java
// 当前代码：一大坨 switch
switch (operator) {
    case PLUS: return arithmetic(left, right, "+");
    case MINUS: return arithmetic(left, right, "-");
    case STAR: return arithmetic(left, right, "*");
    // ... 15 more cases
}
```

**修复**: 按操作类型拆分方法。比较、逻辑、算术各一个方法，由 `evalBinary()` 一行分派。

```java
private Object evalBinary(BinaryExpression expr, Row row, List<Column> columns) {
    switch (expr.getOperator().getType()) {
        case ARITHMETIC: return evalArithmetic(expr, row, columns);
        case COMPARISON: return evalComparison(expr, row, columns);
        case LOGICAL:    return evalLogical(expr, row, columns);
    }
}
```

#### 🟡 P4：DeleteOperator 两遍扫描

**位置**: `DeleteOperator:154-217`

第一遍收集主键，第二遍删除。虽然避免了"遍历时修改"的问题，但全表扫了两遍。

**修复**: 如果 `fullTableScan()` 是惰性迭代器，可以在一遍中先收集 keys，再批量删除。不需要两遍全表扫描。

#### 🟡 P5：InsertOperator 不支持表达式值

**位置**: `InsertOperator:239-256`

```java
private List<Object> evaluateExpressions(List<Expression> expressions) {
    // 只支持 LITERAL，不支持表达式
    if (!(expr instanceof LiteralExpression)) {
        throw new ExecutionException("Only literal values are supported");
    }
}
```

**影响**: `INSERT INTO t VALUES (1+1)` 会报错。
**建议**: 学习阶段可以接受，但应该在注释里明确标注这个限制。

### 1.3 设计亮点

- **Operator 接口**: 干净、简洁、可组合。4 个方法定义清晰。🟢
- **火山模型的组合**: `Scan → Filter → Project` 链条自然流畅。🟢
- **InsertOperator 的类型转换**: `convertValue()` 处理了 INT/VARCHAR/DOUBLE 的转换，虽然不完美但实用。🟢

### 1.4 测试覆盖

测试文件 9 个，测试用例 120+ 个。覆盖了每个 Operator 的核心路径。这是好的。

**但缺少的测试**:
- NULL 值在 WHERE 条件中的行为（SQL 三值逻辑）
- 类型转换失败的边界情况
- 空字符串 vs NULL 的区别
- 并发访问（虽然当前是单线程设计）

---

## 二、Parser 模块

### 2.1 架构评价

**这是三个模块中设计最好的。**

ANTLR 语法 → ASTBuilder (Visitor 模式) → 不可变 Statement/Expression 对象。数据流清晰，每一层职责明确。

### 2.2 致命问题

#### 🟡 P1：Grammar 中 AND/OR 优先级错误 ✅ 已修复 (2026-05-08)

**位置**: `MySQL.g4:171-172`

```antlr4
expression
    : expression op=(AND | OR) expression  # LogicalExpr
    | NOT expression                       # NotExpr
    // ...
```

AND 和 OR 写在同一规则里，意味着它们优先级相同。**SQL 标准中 AND 优先级高于 OR。**

`a = 1 OR b = 2 AND c = 3` 应该被解析为 `a = 1 OR (b = 2 AND c = 3)`，但当前会被解析为 `(a = 1 OR b = 2) AND c = 3`。

**修复方案**: 使用分层规则处理运算符优先级
```antlr4
// 修复后的实现
expression:
    orExpression
;

orExpression:
    orExpression OR andExpression
    | andExpression
;

andExpression:
    andExpression AND notExpression
    | notExpression
;

notExpression:
    NOT notExpression
    | comparisonExpression
;
// ... 其他优先级层级
```
ANTLR 的规则顺序天然定义优先级——后面的规则优先级更高。

#### 🟡 P2：ASTBuilder 的字符串解析有边界 bug

**位置**: `ASTBuilder:272-275`

```java
case STRING:
    return new LiteralExpression(
        token.getText().substring(1, token.getText().length() - 1)
    );
```

如果字符串内容包含转义字符（如 `'it''s'`，SQL 标准中 `''` 表示单引号），这里不会处理转义，直接返回 `it''s` 而不是 `it's`。

**修复**: 添加转义处理 `replace("''", "'")`。

#### 🟡 P3：SQLParser.main() 里的测试代码

**位置**: `SQLParser:115-140`

`main()` 方法里硬编码了 8 条测试 SQL。这是调试残留，不应该出现在正式代码里。

**修复**: 删除 `main()` 方法，测试属于 `src/test/`。

### 2.3 设计亮点

- **不可变 AST**: 所有 Statement 和 Expression 都是 `final` 字段 + `List.copyOf()`。线程安全，不会被意外修改。🟢
- **Expression 层次结构**: `Expression` 接口 → `BinaryExpression`/`ColumnExpression`/`LiteralExpression`/`NotExpression`，简洁清晰。🟢
- **OperatorEnum 的优先级设计**: 虽然当前没被使用（ANTLR 处理优先级），但预留了 `precedence` 字段，为未来手动解析做准备。🟢
- **错误收集器**: `MySQLExceptionCollector` 把 ANTLR 的语法错误转成友好的错误信息，带有行号和列号。🟢

### 2.4 测试覆盖

`SQLParserTest.java` 有 21 个测试用例，覆盖了主要的 SQL 语句类型。

**缺少的测试**:
- 算术表达式（`1 + 2 * 3`）
- 括号表达式（`(a + b) * c`）
- 带表前缀的列名（`users.id`）
- 小数字面量（`3.14`）
- AND/OR 优先级验证

---

## 三、Result 模块

### 3.1 架构评价

一个文件，193 行。`QueryResult` 是一个纯粹的不可变数据容器。

**这就是"Good Taste"。** 简单、直接、没有多余的东西。

### 3.2 致命问题

#### 🟡 P1：toString() 的 O(n*m) 宽度计算

**位置**: `QueryResult:119-130`

```java
// 遍历所有行来计算每列最大宽度
for (Row row : rows) {
    for (int i = 0; i < columns.size(); i++) {
        String strValue = value != null ? value.toString() : "NULL";
        columnWidths[i] = Math.max(columnWidths[i], strValue.length());
    }
}
```

对于百万行结果集，`toString()` 会非常慢。

**实用主义判断**: 这是学习项目，`toString()` 只在调试时调用。**不是问题，不需要修。** 但如果未来要支持大结果集，可以考虑惰性计算或限制显示行数。

### 3.3 设计亮点

- **构造函数防御性拷贝**: `List.copyOf(columns)` + `List.copyOf(rows)`。🟢
- **空结果集处理**: `"Empty set (0.00 sec)"` 模仿 MySQL 输出。🟢
- **MySQL 风格的表格格式**: 完全还原了 MySQL CLI 的输出体验。🟢

### 3.4 测试覆盖

没有独立的测试文件。`QueryResult` 在 `VolcanoExecutorTest` 和集成测试中被间接测试。

**建议**: 至少添加一个测试文件验证：
- 空结果集
- NULL 值显示
- 列宽对齐
- 构造函数的 null 参数校验

---

## 四、跨模块问题

### 4.1 数据流断裂

```
Parser 生成 AST → ExecutionPlan 拿到 Statement → Operator 执行
```

这个流程是通的。但 `VolcanoExecutor.execute()` 只处理 SELECT，INSERT/UPDATE/DELETE/CREATE/DROP 全在 `ExecutionPlan.build()` 里直接调用 `execute()`。

**问题**: `VolcanoExecutor` 名字暗示它是执行器，但实际上 `ExecutionPlan` 才是真正的执行入口。职责模糊。

**建议**: 要么让 `VolcanoExecutor` 处理所有语句类型，要么把它改名为 `SelectExecutor`。

### 4.2 类型系统不统一

`ExpressionEvaluator` 里的类型判断到处都是 `instanceof`：

```java
if (left instanceof Integer && right instanceof Integer) { ... }
if (left instanceof String && right instanceof String) { ... }
```

**问题**: 类型系统散落在 `ExpressionEvaluator`、`InsertOperator.convertValue()`、`UpdateOperator.convertValue()` 三个地方。

**建议**: 提取一个 `TypeSystem` 类，统一处理类型比较和转换。

### 4.3 Operator 接口的语义分裂

`Operator` 接口有两组语义：
1. **查询算子**: `ScanOperator`, `FilterOperator`, `ProjectOperator` — 用 `hasNext()/next()` 返回行
2. **修改算子**: `InsertOperator`, `UpdateOperator`, `DeleteOperator` — `hasNext()` 永远返回 false，需要先调 `execute()`

**问题**: 同一个接口，两种用法。调用者必须知道"这个 Operator 是查询还是修改"。

**建议**: 查询算子和修改算子应该分开。一个返回 `Iterator<Row>`，一个返回 `int affectedRows`。

---

## 五、总结与建议

### 品味排序

1. **Parser**: 🟢 好品味。不可变 AST、Visitor 模式、清晰的层次结构。唯一问题是 AND/OR 优先级。
2. **Result**: 🟢 好品味。简单、不可变、没有多余的东西。
3. **Executor**: 🟡 凑合。架构正确（火山模型），但实现有太多重复代码和职责模糊。

### 优先修复清单

| 优先级 | 问题 | 模块 | 状态 | 预计工作量 |
|--------|------|------|------|------------|
| 🔴 高 | AND/OR 优先级修复 | parser | ✅ 已完成 | 30min |
| 🔴 高 | ScanOperator 惰性化 | executor | ✅ 已完成 | 2h |
| 🔴 高 | 提取列名查找公共方法 | executor | ⏸️ 暂停（测试阻塞）| 1h |
| 🟡 中 | 拆分 evalBinary | executor | 待处理 | 1h |
| 🟡 中 | 删除 SQLParser.main() | parser | 待处理 | 5min |
| 🟡 中 | 字符串转义处理 | parser | 待处理 | 30min |
| 🟢 低 | Operator 接口语义分裂 | executor | 待处理 | 3h |
| 🟢 低 | 类型系统统一 | executor | 待处理 | 2h |

### 一句话总结

> Parser 是好品味，Result 是好品味，Executor 有好品味的心但没有好品味的实现——列名查找重复 6 次是不可接受的。修好 DRY 违反，这个项目就站住了。
