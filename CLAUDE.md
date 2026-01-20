# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

你的所有实现可以简单、简化、但是都必须保持和 MySQL 一样的原理。

**Mini MySQL** - 一个简化版的 MySQL 数据库实现，用于学习数据库内部原理。实现时应该加上必要的注释来理解 MySQL 的原理。

### 核心原则

1. **学习优先**：目标是理解数据库原理，而非生产可用
2. **分层架构**：Parser → Optimizer → Executor → Storage
3. **数据结构优先**：页、表、索引、元数据是核心
4. **消除特殊情况**：统一接口，避免各种"特殊SQL"的if判断
5. **实用主义**：每个模块独立可测，从最简单的开始

## 构建和测试

```bash
# 编译项目（包括ANTLR语法生成）
./gradlew build

# 运行测试
./gradlew test

# 运行单个测试类
./gradlew test --tests com.minimysql.storage.PageTest

# 清理构建产物
./gradlew clean

# 生成ANTLR解析器代码
./gradlew generateGrammarSource
```

## 架构设计

### 核心数据流

```
SQL String
  → Parser (ANTLR) → AST
  → Optimizer → Execution Plan
  → Executor → Iterator Model
  → Storage Engine → Data Pages
  → Result Set
```

### 模块划分

```
src/main/java/com/minimysql/
├── MiniMySQL.java           # 主入口，启动服务
├── parser/                  # SQL 解析层
│   ├── MySQLLexer.g4        # ANTLR词法规则
│   ├── MySQLParser.g4       # ANTLR语法规则
│   ├── ASTNode.java         # 抽象语法树节点
│   └── Statement.java       # SQL语句接口
├── optimizer/               # 查询优化层
│   ├── LogicalPlan.java     # 逻辑执行计划
│   ├── PhysicalPlan.java    # 物理执行计划
│   └── CostEstimator.java   # 代价估算（可选）
├── executor/                # 执行引擎
│   ├── Executor.java        # 执行器接口
│   ├── VolcanoExecutor.java # 火山模型（迭代器）
│   ├── VectorizedExecutor.java # 向量化执行（可选）
│   └── operators/           # 算子实现
│       ├── ScanOperator.java
│       ├── FilterOperator.java
│       ├── ProjectOperator.java
│       └── JoinOperator.java
├── storage/                 # 存储引擎
│   ├── StorageEngine.java   # 存储引擎接口
│   ├── buffer/              # 缓冲池管理
│   │   ├── BufferPool.java  # 缓冲池（LRU淘汰）
│   │   └── PageFrame.java   # 页帧
│   ├── page/                # 页管理
│   │   ├── Page.java        # 页接口（16KB）
│   │   ├── DataPage.java    # 数据页
│   │   ├── IndexPage.java   # 索引页
│   │   └── PageManager.java # 页分配器
│   ├── index/               # 索引结构
│   │   ├── Index.java       # 索引接口
│   │   ├── BPlusTree.java   # B+树实现
│   │   └── HashIndex.java   # 哈希索引（可选）
│   ├── table/               # 表管理
│   │   ├── Table.java       # 表定义
│   │   ├── Row.java         # 行数据
│   │   └── Column.java      # 列定义
│   └── transaction/         # 事务管理
│       ├── Transaction.java # 事务
│       ├── LockManager.java # 锁管理器
│       └── RecoveryLog.java # WAL日志（可选）
└── metadata/                # 元数据管理
    ├── SystemTables.java    # 系统表定义（SYS_TABLES, SYS_COLUMNS）
    ├── TableMetadata.java   # 表元数据DTO
    ├── ColumnMetadata.java  # 列元数据DTO
    └── SchemaManager.java   # Schema管理器
```

## 开发指南

### 实现哲学（Linus 视角）

**"Bad programmers worry about the code. Good programmers worry about data structures."**

1. **数据结构优先**
   - 先设计 `Page`（16KB固定大小）和 `BufferPool`（LRU缓存）
   - 再设计 `Table` 和 `Row` 如何在页中存储
   - 最后设计 SQL 如何操作这些数据结构

2. **消除特殊情况**
   - 不要为"不同SQL类型"写不同的处理逻辑
   - 统一使用 `Operator` 接口：Scan → Filter → Project
   - 所有操作都是算子的组合，没有"特殊优化"

3. **简单直接**
   - 不要实现完整的查询优化器，先支持最基本的 Scan
   - 不要实现复杂的并发控制，先支持单线程
   - 不要实现完整的SQL标准，只支持核心子集

### 实现顺序建议

从底向上实现，每层可独立测试：

1. **Storage 层**
   - [x] Page：固定16KB内存块，支持读写
   - [x] BufferPool：LRU缓存，支持页换入换出
   - [x] DataPage：在页中存储行数据（简单槽位结构）
   - [x] PageManager：管理页的分配和释放

2. **Table 层**
   - [x] Column：列定义（类型、长度、是否可空）
   - [x] Row：行数据（字节数组+偏移量）
   - [x] Table：表定义（列集合+主键）

3. **Index 层**
   - [x] BPlusTree：基本的B+树实现
   - [x] 聚簇索引（ClusteredIndex）
   - [x] 二级索引（SecondaryIndex）
   - [x] 在表中集成主键和二级索引

4. **元数据层**
   - [x] 系统表定义（SYS_TABLES, SYS_COLUMNS）
   - [x] SchemaManager：管理表元数据的创建、删除、查询
   - [x] 元数据持久化到系统表
   - [x] 集成到 InnoDBStorageEngine
   - [ ] 实现元数据加载（依赖 Table.fullTableScan()）

5. **Parser 层**
   - [x] ANTLR语法：支持最基本的 SELECT、INSERT、UPDATE、DELETE、CREATE TABLE、DROP TABLE
   - [x] AST到Statement的转换
   - [x] Statement接口体系（Create/Drop/Select/Insert/Update/Delete）
   - [x] Expression接口体系（列引用/字面量/二元运算/NOT运算）
   - [x] SQLParser对外接口
   - [x] 完整的单元测试（15个测试用例全部通过）

6. **Executor 层**
   - [ ] ScanOperator：全表扫描
   - [ ] FilterOperator：条件过滤
   - [ ] ProjectOperator：列投影
   - [ ] 火山模型：每个Operator实现 `next()` 和 `hasNext()`

7. **元数据**
   - [x] Catalog：系统表（SYS_TABLES, SYS_COLUMNS）
   - [x] SchemaManager：元数据管理器
   - [x] 持久化到系统表（不使用外部文件）

### 测试策略

```bash
# 每个模块独立测试
./gradlew test --tests com.minimysql.storage.PageTest
./gradlew test --tests com.minimysql.metadata.SchemaManagerTest
./gradlew test --tests com.minimysql.executor.ScanOperatorTest

# 集成测试：端到端SQL执行
./gradlew test --tests com.minimysql.integration.EndToEndTest
```

### 元数据管理设计

**核心原理**：
- 采用 **MySQL InnoDB 风格**的系统表设计
- 元数据存储在专门的系统表中（`SYS_TABLES`, `SYS_COLUMNS`）
- 对应 MySQL 的 `information_schema` 库

**系统表结构**：
1. **SYS_TABLES**（系统表定义）
   - `table_id` (INT): 表ID
   - `table_name` (VARCHAR(128)): 表名

2. **SYS_COLUMNS**（列定义）
   - `table_id` (INT): 表ID
   - `column_name` (VARCHAR(128)): 列名
   - `column_type` (VARCHAR(32)): 列类型（INT, VARCHAR等）
   - `column_length` (INT): 类型长度（仅VARCHAR有效）
   - `nullable` (BOOLEAN): 是否可空
   - `column_position` (INT): 列位置

**设计亮点**：
- ✅ **"Good taste"**: 系统表就是普通的 `Table`，没有特殊处理
- ✅ **消除特殊情况**: 系统表创建后，可以用同样的 API 操作
- ✅ **数据结构优先**: 元数据直接存储在系统表中，不依赖外部配置文件
- ✅ **实用主义**: 只实现必要的系统表，不是完整的 `information_schema`

**鸡生蛋问题**：
系统表本身也需要元数据，如何解决？
- **解决方案**: 系统表在初始化时硬编码创建，元数据表本身不存储在 `SYS_TABLES` 中
- **实现**: `SchemaManager` 通过直接创建 `Table` 对象绕过 `StorageEngine.createTable()` 的检查

**未完成功能**（TODO）：
1. **元数据加载**: 重启后从系统表加载表定义（需要实现 `Table.fullTableScan()`）
2. **B+树删除**: 目前 `dropTable()` 只删除 `SYS_TABLES`，`SYS_COLUMNS` 会残留
3. **索引元数据**: 添加 `SYS_INDEXES` 系统表管理索引信息

## TODO 清单（选择性实现）

### 阶段1：核心存储（必须实现）

- [x] 项目结构和构建系统
- [x] Page 接口和 DataPage 实现
- [x] BufferPool（LRU淘汰算法）
- [x] PageManager（页分配器）
- [x] Row 和 Column 数据结构
- [x] Table 表定义
- [ ] 简单的文件存储（每页一个文件或单一数据文件）

### 阶段2：基础索引（必须实现）

- [x] B+树节点结构
- [x] B+树插入、查找、删除
- [x] 主键索引（聚簇索引）
- [x] 二级索引（SecondaryIndex）
- [x] 在 Table 中集成索引
- [ ] B+树删除功能完整实现（节点合并、借位）

### 阶段3：SQL解析（必须实现）

- [x] ANTLR 语法规则（简化版）
- [x] SELECT 语句解析（支持 WHERE）
- [x] INSERT 语句解析
- [x] UPDATE 语句解析
- [x] DELETE 语句解析
- [x] CREATE TABLE 语句解析
- [x] DROP TABLE 语句解析
- [x] AST 节点定义（Statement 和 Expression 接口体系）
- [x] ASTBuilder（ANTLR AST → Statement 转换）
- [x] SQLParser 对外接口
- [x] 单元测试（15个测试用例）

### 阶段4：执行引擎（必须实现）

- [ ] Operator 接口
- [ ] ScanOperator（全表扫描）
- [ ] FilterOperator（WHERE 条件）
- [ ] ProjectOperator（SELECT 列）
- [ ] VolcanoExecutor（迭代器模型）
- [ ] 简单的查询计划生成

### 阶段5：元数据管理（必须实现）

- [x] 系统表定义（SYS_TABLES, SYS_COLUMNS）
- [x] SchemaManager（元数据管理器）
- [x] 表结构持久化到系统表
- [x] 集成到 InnoDBStorageEngine
- [x] 系统表保护机制
- [ ] 元数据加载功能（实现 Table.fullTableScan() 后完成）
- [ ] 索引元数据管理（SYS_INDEXES）
- [ ] 完善删除功能（依赖 BPlusTree.delete() 实现）

### 阶段6：事务和并发（可选）

- [ ] Transaction 接口
- [ ] 简单的锁管理器（表级锁）
- [ ] 行级锁（可选）
- [ ] ACID 基础支持
- [ ] WAL 日志（可选）

### 阶段7：查询优化（可选）

- [ ] LogicalPlan
- [ ] PhysicalPlan
- [ ] 简单的规则优化（谓词下推）
- [ ] 索引选择
- [ ] Join 顺序优化

### 阶段8：高级特性（可选）

- [ ] JoinOperator（Nested Loop Join）
- [ ] SortOperator（ORDER BY）
- [ ] AggregationOperator（GROUP BY）
- [ ] 子查询支持

### 阶段9：性能优化（可选）

- [ ] 向量化执行
- [ ] 批量插入
- [ ] 并行查询
- [ ] 自适应哈希索引

### 阶段10：网络协议（可选）

- [ ] MySQL 协议支持
- [ ] TCP 服务器
- [ ] 连接管理
- [ ] 权限认证

## 常见问题

### Q: 如何测试单个功能？
```bash
# 创建测试类
./gradlew test --tests com.minimysql.storage.PageTest

# 或者运行特定测试方法
./gradlew test --tests "*.PageTest.testReadWrite"
```

### Q: ANTLR 语法修改后如何重新生成？
```bash
./gradlew clean generateGrammarSource build
```

### Q: 数据存储在哪里？
默认在 `./data/` 目录，每个表对应一个文件。

### Q: 如何调试单条 SQL？
在 `src/test/java` 下创建集成测试，直接调用 Executor。

## 关键设计决策

1. **页大小固定为16KB**：与MySQL InnoDB一致，减少内存碎片
2. **火山模型执行**：简单、可组合，适合学习
3. **LRU缓冲池**：经典的页面置换算法
4. **B+树索引**：最常用的数据库索引结构
5. **单文件存储**：简化实现，每个表一个 `.db` 文件

## 避免的陷阱

**⚠️ Linus 的警告：**

1. **不要过度优化**：这是学习项目，不是生产系统
2. **不要实现所有SQL特性**：只支持核心子集
3. **不要过早优化性能**：先让代码跑起来，再优化
4. **不要写复杂的"备用代码"**：没有回退逻辑，没有兼容模式
5. **不要试图超越MySQL**：目标是理解原理，不是替代MySQL

**"Theory and practice sometimes clash. Theory loses. Every single time."**

实现一个能工作的简单系统，而不是一个理论上完美但无法运行的复杂系统。
