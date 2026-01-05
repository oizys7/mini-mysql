# Mini MySQL

A simplified MySQL implementation for learning database internals.

## 概述

这是一个用 Java 实现的简化版 MySQL 数据库，目的是学习数据库内部原理。

**注意**：这不是生产级别的实现，仅供学习使用。

## 快速开始

### 环境要求

- Java 21+
- Gradle 8.0+

### 构建

```bash
./gradlew build
```

### 运行测试

```bash
./gradlew test
```

## 项目结构

```
mini-mysql/
├── parser/          # SQL 解析器（ANTLR）
├── optimizer/       # 查询优化器
├── executor/        # 执行引擎（火山模型）
├── storage/         # 存储引擎
│   ├── buffer/      # 缓冲池
│   ├── page/        # 页管理
│   ├── index/       # B+树索引
│   └── transaction/ # 事务管理
└── metadata/        # 元数据管理
```

## 实现进度

当前处于**项目初始化阶段**，具体实现进度见 [CLAUDE.md](./CLAUDE.md) 中的 TODO 清单。

## 核心特性（计划中）

- ✅ 16KB 固定页大小
- ✅ LRU 缓冲池
- ✅ B+树索引
- ✅ 火山模型执行引擎
- ✅ 支持 SELECT、INSERT、CREATE TABLE
- ⏳ 事务支持（可选）
- ⏳ 查询优化（可选）

## 学习资源

- 《数据库系统概念》
- 《数据库系统实现》
- MySQL Internals Manual
- CMU 15-445 课程

## 许可证

MIT License
