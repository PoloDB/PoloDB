
# PoloDB

PoloDB 是一个嵌入式 NoSQL 数据库。

# 特性

- 嵌入式（不需要独立进程）
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

PoloDB 的 API 和 MongoDB 类似，易学易用。

# 开发计划

以下特性会按计划一个一个来：

- [ ] MongoDB 的 API
- [ ] 命令行工具
- [ ] 移动平台的编译
  - [ ] iOS
  - [ ] Android
  - [ ] Flutter
- [ ] 语言绑定
  - [ ] C
  - [ ] Go
  - [ ] Objective-C
  - [ ] Swift
  - [ ] Python
  - [ ] Ruby
  - [ ] Elixir
  - [ ] Node.js
  - [ ] Java
  - [ ] Kotlin
  - [ ] Dart
- [ ] 多线程支持
- [ ] 拓展 API
  - [ ] 数据加密
  - [ ] JavaScript 引擎
- [ ] 可视化工具

# FAQ

## PoloDB 支持多线程吗？

暂时**不支持**，不过已经在需求池了。
