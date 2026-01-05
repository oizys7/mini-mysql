# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

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
    ├── Catalog.java         # 系统表（表、列、索引信息）
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
   - [ ] Page：固定16KB内存块，支持读写
   - [ ] BufferPool：LRU缓存，支持页换入换出
   - [ ] DataPage：在页中存储行数据（简单槽位结构）
   - [ ] PageManager：管理页的分配和释放

2. **Table 层**
   - [ ] Column：列定义（类型、长度、是否可空）
   - [ ] Row：行数据（字节数组+偏移量）
   - [ ] Table：表定义（列集合+主键）

3. **Index 层**
   - [ ] BPlusTree：基本的B+树实现
   - [ ] 在表中添加主键索引

4. **Parser 层**
   - [ ] ANTLR语法：支持最基本的 SELECT、INSERT、CREATE TABLE
   - [ ] AST到Statement的转换

5. **Executor 层**
   - [ ] ScanOperator：全表扫描
   - [ ] FilterOperator：条件过滤
   - [ ] ProjectOperator：列投影
   - [ ] 火山模型：每个Operator实现 `next()` 和 `hasNext()`

6. **元数据**
   - [ ] Catalog：内存中存储表结构信息
   - [ ] 持久化到文件（简单JSON或二进制）

### 测试策略

```bash
# 每个模块独立测试
./gradlew test --tests com.minimysql.storage.PageTest
./gradlew test --tests com.minimysql.executor.ScanOperatorTest

# 集成测试：端到端SQL执行
./gradlew test --tests com.minimysql.integration.EndToEndTest
```

## TODO 清单（选择性实现）

### 阶段1：核心存储（必须实现）

- [x] 项目结构和构建系统
- [ ] Page 接口和 DataPage 实现
- [ ] BufferPool（LRU淘汰算法）
- [ ] PageManager（页分配器）
- [ ] Row 和 Column 数据结构
- [ ] Table 表定义
- [ ] 简单的文件存储（每页一个文件或单一数据文件）

### 阶段2：基础索引（必须实现）

- [ ] B+树节点结构
- [ ] B+树插入、查找、删除
- [ ] 主键索引
- [ ] 在 Table 中集成索引

### 阶段3：SQL解析（必须实现）

- [ ] ANTLR 语法规则（简化版）
- [ ] SELECT 语句解析（支持 WHERE）
- [ ] INSERT 语句解析
- [ ] CREATE TABLE 语句解析
- [ ] AST 节点定义

### 阶段4：执行引擎（必须实现）

- [ ] Operator 接口
- [ ] ScanOperator（全表扫描）
- [ ] FilterOperator（WHERE 条件）
- [ ] ProjectOperator（SELECT 列）
- [ ] VolcanoExecutor（迭代器模型）
- [ ] 简单的查询计划生成

### 阶段5：元数据管理（必须实现）

- [ ] Catalog（系统表）
- [ ] SchemaManager
- [ ] 表结构持久化

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
- [ ] 视图
- [ ] 存储过程

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
