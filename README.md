# search-opensearch

`search` 的 OpenSearch 驱动。

驱动名：`opensearch`

## 使用

```go
import _ "github.com/bamgoo/search-opensearch"
```

```toml
[search]
driver = "opensearch"
prefix = "demo_"

[search.setting]
server = "http://127.0.0.1:9200"
username = ""
password = ""
api_key = ""
```

## 配置项

- `server`：OpenSearch 地址
- `username/password`：Basic Auth（可选）
- `api_key`：API Key（可选，优先于 basic auth）
- `prefix`：索引名前缀（可选）
- `timeout`：HTTP 超时（例如 `5s`）

## 映射说明

1. 统一 `Search DSL` 会映射到 bool query + filter + sort + aggs + highlight。
2. `facets` 映射为 `terms aggregation`。
3. `Upsert/Delete` 使用 `_bulk`。
