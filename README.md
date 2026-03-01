# search-opensearch

`search-opensearch` 是 `search` 模块的 `opensearch` 驱动。

## 安装

```bash
go get github.com/infrago/search@latest
go get github.com/infrago/search-opensearch@latest
```

## 接入

```go
import (
    _ "github.com/infrago/search"
    _ "github.com/infrago/search-opensearch"
    "github.com/infrago/infra"
)

func main() {
    infra.Run()
}
```

## 配置示例

```toml
[search]
driver = "opensearch"
```

## 公开 API（摘自源码）

- `func (d *openSearchDriver) Connect(inst *search.Instance) (search.Connection, error)`
- `func (c *openSearchConnection) Open() error  { return nil }`
- `func (c *openSearchConnection) Close() error { return nil }`
- `func (c *openSearchConnection) Capabilities() search.Capabilities`
- `func (c *openSearchConnection) SyncIndex(name string, index search.Index) error`
- `func (c *openSearchConnection) Clear(name string) error`
- `func (c *openSearchConnection) Upsert(index string, rows []Map) error`
- `func (c *openSearchConnection) Delete(index string, ids []string) error`
- `func (c *openSearchConnection) Search(index string, query search.Query) (search.Result, error)`
- `func (c *openSearchConnection) Count(index string, query search.Query) (int64, error)`

## 排错

- driver 未生效：确认模块段 `driver` 值与驱动名一致
- 连接失败：检查 endpoint/host/port/鉴权配置
