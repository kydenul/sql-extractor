# sql-extractor

[![Go Version](https://img.shields.io/badge/Go-1.23%2B-blue)](https://golang.org/doc/devel/release.html#go1.23)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

sql-extractor 是一个高性能的 SQL 解析和转换工具，它可以将 SQL 语句转换为参数化模板，并提取相关的表信息和参数值。该工具基于 TiDB 的 SQL 解析器，支持复杂的 SQL 语句分析。

## 功能特性

- 支持多种 SQL 操作类型：SELECT、INSERT、UPDATE、DELETE、SHOW CREATE TABLE、SHOW CREATE DATABASE、SHOW DATABASES、SHOW TABLES、SHOW COLUMNS、SHOW INDEX、SHOW STATUS、SHOW VARIABLES、SHOW PROCESSLIST、SHOW TABLE STATUS、SHOW WARNINGS、SHOW ERRORS
- SQL 语句参数化：将字面值转换为占位符(`?`)
- 表信息提取：捕获查询中使用的 schema 和表名
  - 分库分表支持：支持分库分表的表名提取和模板化，例如 `db_1`.`tb_23` 会被转换为 `db_?`.`tb_?`
- 参数提取：按出现顺序收集 SQL 中的字面值
- 多语句支持：可以处理以分号分隔的多个 SQL 语句
- 线程安全：使用 sync.Pool 进行并发处理
- 支持复杂 SQL 特性：
  - JOIN 操作（LEFT JOIN、RIGHT JOIN、INNER JOIN）
  - 子查询
  - 聚合函数
  - 各种 SQL 表达式（LIKE、IN、BETWEEN 等）

## 性能优化

- 使用 sync.Pool 复用 visitor 对象，减少内存分配
- 预分配适当大小的切片，避免频繁扩容
- 使用 strings.Builder 进行字符串拼接

## 系统要求

- Go 1.23 或更高版本
- 依赖包：
  - github.com/pingcap/tidb/pkg/parser
  - github.com/samber/lo

## 安装

```bash
go install github.com/kydenul/sql-extractor@latest
```

## API 文档

### Extractor

主要的提取器结构体，用于处理 SQL 语句。

```go
type Extractor struct {
    // 包含已过滤或未导出的字段
}

// RawSQL returns the raw SQL.
func (e *Extractor) RawSQL() string 

// SetRawSQL sets the raw SQL.
func (e *Extractor) SetRawSQL(sql string) 

// TemplatizedSQL returns the templatized SQL.
func (e *Extractor) TemplatizedSQL() []string 

// Params returns the parameters.
func (e *Extractor) Params() [][]any 

// TableInfos returns the table infos.
func (e *Extractor) TableInfos() [][]*models.TableInfo 

// OpType returns the operation type.
func (e *Extractor) OpType() []models.SQLOpType 

// doHash calculates the hash of the templatized SQL.
func (e *Extractor) doHash(fn ...func([]byte) string) 

// TemplatizedSQLHash returns the hash of the templatized SQL.
//
// Default hash function is sha256.
func (e *Extractor) TemplatizedSQLHash(fn ...func([]byte) string) []string 

// HasParamMarker returns whether the SQLs contains parameter markers.
func (e *Extractor) HasParamMarker() []bool 

// Extract extracts information from the raw SQL string. It extracts the templatized
// SQL, parameters, table information, and operation type.
func (e *Extractor) Extract() (err error) 
```

### TableInfo

表信息结构体，包含 schema 和表名信息。

```go
type TableInfo struct {
    templatizedSchema    string // templated schema, e.g. db_?
    templatizedTableName string // templated table name, e.g. tb_?
    
    schema    string // original schema, e.g. db_23
    tableName string // original table name, e.g. tb_10
}
```

## 贡献指南

1. Fork 本仓库
2. 创建您的特性分支 (`git checkout -b feature/amazing-feature`)
3. 提交您的更改 (`git commit -m 'feat: add some amazing feature'`)
4. 推送到分支 (`git push origin feature/amazing-feature`)
5. 开启一个 Pull Request

## 许可证

本项目采用 MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

## 作者

- [@kydenul](https://github.com/kydenul)

## 致谢

- [TiDB Parser](https://github.com/pingcap/tidb) - SQL 解析器
