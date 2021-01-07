
# PoloDB

[![Crates.io](https://img.shields.io/crates/v/polodb_core.svg)](https://crates.io/crates/polodb_core)
[![npm version](https://img.shields.io/npm/v/polodb.svg)](https://www.npmjs.com/package/polodb)

PoloDB 是一个嵌入式 NoSQL 数据库。

# 特性

- 嵌入式（不需要独立进程，不需要跨进程调用）
- 无依赖（除了 libc，系统自带）
- NoSQL
- 与 MongoDB 相似的 API
- 跨平台
- 数据储存在一个文件里

# 介绍

PoloDB 是一个 Rust 实现的轻量级的嵌入式数据库。

PoloDB 几乎没有依赖（除了 libc，系统自带），
所以可以很好地跨平台（也因为 Rust 语言本身）。

PoloDB 所有数据都储存在一个文件里。PoloDB
的文件格式是稳定的，跨平台而且向后兼容的。

PoloDB 的 API 和 [MongoDB](https://www.mongodb.com/) 类似，易学易用。

# 支持的平台

理论上来说，Rust 编译器支持的平台，PoloDB 都能支持。
但是鉴于 PoloDB 是一个个人项目，个人精力有限，我只能支持以下平台：

- macOS 10.15 x64
- Linux x64 (Tested on Fedora 30)
- Windows 10 x64

# 使用方法

- [Rust](https://docs.rs/polodb_core)
- [Node.js](./docs/zh-CN/Node.js/READEME.md)
- [Python](./docs/zh-CN/Python/READEME.md)

# 开发计划

以下特性会按计划一个一个来：

- [x] 基本数据库功能
  - [x] CURD
  - [x] 事务性提交
  - [ ] 索引
  - [ ] 聚合函数
- [x] 命令行工具
- [ ] 移动平台的编译
  - [ ] iOS
  - [ ] Android
  - [ ] Flutter
- [ ] 语言绑定
  - [x] C/C++
  - [ ] Go
  - [x] Swift([Repo](https://github.com/vincentdchan/SwiftyPoloDB))
  - [x] Python
  - [x] Node.js
  - [ ] Java/Kotlin
  - [ ] Dart
- [ ] 大量测试
- [ ] 多线程支持
- [ ] 拓展 API
  - [ ] 数据加密
  - [ ] JavaScript 引擎
- [ ] 可视化工具

# 贡献指南

| 模块 | 路径 | 描述 |
| ----| --- | ---- |
| Core | `src/polodb_core`  | 数据库功能的核心实现 |
| C Library | `src/polodb_clib` | 数据库的 C FFI 实现 |
| CLI tool | `src/polodb_cli` | 命令行工具 |
| Node.js bindings | `polodb.js` | Node.js 绑定，用 N-API 实现 |
| Python bindings | `pypolodb` | CPython 绑定，Extension API 实现 |
| Swift bindings | `SwiftyPoloDB` | Swift |
