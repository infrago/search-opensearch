package search_opensearch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/bamgoo/bamgoo"
	. "github.com/bamgoo/base"
	"github.com/bamgoo/search"
)

type openSearchDriver struct{}

type openSearchConnection struct {
	server string
	user   string
	pass   string
	key    string
	prefix string
	client *http.Client
}

func init() {
	bamgoo.Register("opensearch", &openSearchDriver{})
}

func (d *openSearchDriver) Connect(inst *search.Instance) (search.Connection, error) {
	server := pickString(inst.Config.Setting, "server", "host", "url")
	if server == "" {
		server = "http://127.0.0.1:9200"
	}
	timeout := 5 * time.Second
	if inst.Config.Timeout > 0 {
		timeout = inst.Config.Timeout
	}
	if v, ok := inst.Config.Setting["timeout"].(string); ok {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			timeout = d
		}
	}
	prefix := inst.Config.Prefix
	if prefix == "" {
		prefix = pickString(inst.Config.Setting, "prefix", "index_prefix")
	}
	return &openSearchConnection{
		server: strings.TrimRight(server, "/"),
		user:   pickString(inst.Config.Setting, "username", "user"),
		pass:   pickString(inst.Config.Setting, "password", "pass"),
		key:    pickString(inst.Config.Setting, "api_key", "apikey", "key"),
		prefix: prefix,
		client: &http.Client{Timeout: timeout},
	}, nil
}

func (c *openSearchConnection) Open() error  { return nil }
func (c *openSearchConnection) Close() error { return nil }

func (c *openSearchConnection) CreateIndex(name string, index search.Index) error {
	idx := c.indexName(name)
	payload := Map{}
	if index.Setting != nil {
		payload = cloneMap(index.Setting)
	}
	if len(index.Attributes) > 0 {
		mappings, ok := payload["mappings"].(Map)
		if !ok || mappings == nil {
			mappings = Map{}
		}
		properties := Map{}
		if pp, ok := mappings["properties"].(Map); ok && pp != nil {
			properties = cloneMap(pp)
		}
		for field, v := range index.Attributes {
			properties[field] = Map{"type": openSearchFieldType(v.Type)}
		}
		mappings["properties"] = properties
		payload["mappings"] = mappings
	}
	_, err := c.request(http.MethodPut, "/"+url.PathEscape(idx), payload)
	if err != nil && strings.Contains(strings.ToLower(err.Error()), "resource_already_exists_exception") {
		return nil
	}
	return err
}

func (c *openSearchConnection) DropIndex(name string) error {
	_, err := c.request(http.MethodDelete, "/"+url.PathEscape(c.indexName(name)), nil)
	return err
}

func (c *openSearchConnection) Upsert(index string, docs []search.Document) error {
	if len(docs) == 0 {
		return nil
	}
	idx := c.indexName(index)
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, doc := range docs {
		if doc.ID == "" {
			continue
		}
		action := Map{"index": Map{"_index": idx, "_id": doc.ID}}
		_ = enc.Encode(action)
		_ = enc.Encode(doc.Payload)
	}
	return c.bulk(buf.Bytes())
}

func (c *openSearchConnection) Delete(index string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	idx := c.indexName(index)
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, id := range ids {
		action := Map{"delete": Map{"_index": idx, "_id": id}}
		_ = enc.Encode(action)
	}
	return c.bulk(buf.Bytes())
}

func (c *openSearchConnection) Search(index string, query search.Query) (search.Result, error) {
	body := buildSearchBody(query)
	respBytes, err := c.request(http.MethodPost, "/"+url.PathEscape(c.indexName(index))+"/_search", body)
	if err != nil {
		return search.Result{}, err
	}

	var resp struct {
		Took int64 `json:"took"`
		Hits struct {
			Total struct {
				Value int64 `json:"value"`
			} `json:"total"`
			Hits []struct {
				ID        string  `json:"_id"`
				Score     float64 `json:"_score"`
				Source    Map     `json:"_source"`
				Highlight Map     `json:"highlight"`
			} `json:"hits"`
		} `json:"hits"`
		Aggregations Map `json:"aggregations"`
	}
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		return search.Result{}, err
	}

	hits := make([]search.Hit, 0, len(resp.Hits.Hits))
	for _, hit := range resp.Hits.Hits {
		payload := hit.Source
		if payload == nil {
			payload = Map{}
		}
		applyHighlight(payload, hit.Highlight)
		if len(query.Fields) > 0 {
			payload = pickFields(payload, query.Fields)
		}
		hits = append(hits, search.Hit{ID: hit.ID, Score: hit.Score, Payload: payload})
	}

	facets := map[string][]search.Facet{}
	for _, field := range query.Facets {
		agg, ok := resp.Aggregations[field].(Map)
		if !ok {
			continue
		}
		buckets, ok := agg["buckets"].([]Any)
		if !ok {
			continue
		}
		vals := make([]search.Facet, 0, len(buckets))
		for _, one := range buckets {
			bucket, ok := one.(Map)
			if !ok {
				continue
			}
			vals = append(vals, search.Facet{Field: field, Value: fmt.Sprintf("%v", bucket["key"]), Count: toInt64(bucket["doc_count"])})
		}
		facets[field] = vals
	}

	return search.Result{Total: resp.Hits.Total.Value, Took: resp.Took, Hits: hits, Facets: facets, Raw: resp.Aggregations}, nil
}

func (c *openSearchConnection) Count(index string, query search.Query) (int64, error) {
	body := buildSearchBody(query)
	body["size"] = 0
	respBytes, err := c.request(http.MethodPost, "/"+url.PathEscape(c.indexName(index))+"/_count", Map{"query": body["query"]})
	if err != nil {
		return 0, err
	}
	var resp struct {
		Count int64 `json:"count"`
	}
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		return 0, err
	}
	return resp.Count, nil
}

func (c *openSearchConnection) Suggest(index string, text string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 10
	}
	q := search.Query{Keyword: strings.TrimSpace(text), Limit: limit, Offset: 0}
	res, err := c.Search(index, q)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(res.Hits))
	seen := map[string]struct{}{}
	for _, hit := range res.Hits {
		if hit.ID == "" {
			continue
		}
		if _, ok := seen[hit.ID]; ok {
			continue
		}
		seen[hit.ID] = struct{}{}
		out = append(out, hit.ID)
	}
	return out, nil
}

func (c *openSearchConnection) bulk(body []byte) error {
	req, err := http.NewRequest(http.MethodPost, c.server+"/_bulk", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	c.withAuth(req)
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	bts, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("opensearch bulk failed: %s", strings.TrimSpace(string(bts)))
	}
	return nil
}

func (c *openSearchConnection) request(method, path string, body Any) ([]byte, error) {
	var reader io.Reader
	if body != nil {
		bts, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(bts)
	}
	req, err := http.NewRequest(method, c.server+path, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	c.withAuth(req)
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	bts, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("opensearch %s %s failed: %s", method, path, strings.TrimSpace(string(bts)))
	}
	return bts, nil
}

func (c *openSearchConnection) withAuth(req *http.Request) {
	if c.key != "" {
		req.Header.Set("Authorization", "ApiKey "+c.key)
		return
	}
	if c.user != "" {
		req.SetBasicAuth(c.user, c.pass)
	}
}

func (c *openSearchConnection) indexName(name string) string {
	if c.prefix == "" {
		return name
	}
	return c.prefix + name
}

func buildSearchBody(query search.Query) Map {
	must := make([]Any, 0)
	if strings.TrimSpace(query.Keyword) != "" {
		must = append(must, Map{"multi_match": Map{"query": query.Keyword, "fields": []string{"*"}}})
	}
	filters := make([]Any, 0)
	for _, f := range query.Filters {
		if q := toFilterQuery(f); q != nil {
			filters = append(filters, q)
		}
	}
	boolQuery := Map{}
	if len(must) > 0 {
		boolQuery["must"] = must
	}
	if len(filters) > 0 {
		boolQuery["filter"] = filters
	}
	if len(boolQuery) == 0 {
		boolQuery["must"] = []Any{Map{"match_all": Map{}}}
	}

	body := Map{
		"from":  query.Offset,
		"size":  query.Limit,
		"query": Map{"bool": boolQuery},
	}
	if len(query.Sorts) > 0 {
		sorts := make([]Any, 0, len(query.Sorts))
		for _, s := range query.Sorts {
			order := "asc"
			if s.Desc {
				order = "desc"
			}
			sorts = append(sorts, Map{s.Field: Map{"order": order}})
		}
		body["sort"] = sorts
	}
	if len(query.Facets) > 0 {
		aggs := Map{}
		for _, field := range query.Facets {
			aggs[field] = Map{"terms": Map{"field": field}}
		}
		body["aggs"] = aggs
	}
	if len(query.Highlight) > 0 {
		fields := Map{}
		for _, field := range query.Highlight {
			fields[field] = Map{}
		}
		body["highlight"] = Map{"fields": fields}
	}
	if len(query.Fields) > 0 {
		body["_source"] = query.Fields
	}
	return body
}

func toFilterQuery(f search.Filter) Map {
	op := strings.ToLower(strings.TrimSpace(f.Op))
	if op == "" {
		op = "eq"
	}
	switch op {
	case "eq", "=":
		return Map{"term": Map{f.Field: f.Value}}
	case "in":
		vals := f.Values
		if len(vals) == 0 && f.Value != nil {
			vals = []Any{f.Value}
		}
		return Map{"terms": Map{f.Field: vals}}
	case "gt", ">", "gte", ">=", "lt", "<", "lte", "<=", "range":
		r := Map{}
		switch op {
		case "gt", ">":
			r["gt"] = f.Value
		case "gte", ">=":
			r["gte"] = f.Value
		case "lt", "<":
			r["lt"] = f.Value
		case "lte", "<=":
			r["lte"] = f.Value
		case "range":
			if f.Min != nil {
				r["gte"] = f.Min
			}
			if f.Max != nil {
				r["lte"] = f.Max
			}
		}
		if len(r) > 0 {
			return Map{"range": Map{f.Field: r}}
		}
	}
	return nil
}

func pickString(m Map, keys ...string) string {
	if m == nil {
		return ""
	}
	for _, key := range keys {
		if v, ok := m[key].(string); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func cloneMap(src Map) Map {
	if src == nil {
		return Map{}
	}
	out := Map{}
	for k, v := range src {
		out[k] = v
	}
	return out
}

func pickFields(payload Map, fields []string) Map {
	if payload == nil {
		return Map{}
	}
	if len(fields) == 0 {
		return cloneMap(payload)
	}
	out := Map{}
	for _, field := range fields {
		if v, ok := payload[field]; ok {
			out[field] = v
		}
	}
	return out
}

func openSearchFieldType(t string) string {
	switch strings.ToLower(strings.TrimSpace(t)) {
	case "bool", "boolean":
		return "boolean"
	case "int", "int8", "int16", "int32":
		return "integer"
	case "int64", "uint", "uint8", "uint16", "uint32", "uint64":
		return "long"
	case "float", "float32":
		return "float"
	case "float64", "decimal", "number":
		return "double"
	case "timestamp", "datetime", "date", "time":
		return "date"
	case "map", "json", "jsonb":
		return "object"
	default:
		return "keyword"
	}
}

func applyHighlight(payload Map, hl Map) {
	if payload == nil || hl == nil {
		return
	}
	for field, value := range hl {
		switch vv := value.(type) {
		case []Any:
			if len(vv) > 0 {
				payload[field] = vv[0]
			}
		case []string:
			if len(vv) > 0 {
				payload[field] = vv[0]
			}
		case string:
			payload[field] = vv
		}
	}
}

func toInt64(v Any) int64 {
	switch vv := v.(type) {
	case int:
		return int64(vv)
	case int64:
		return vv
	case float64:
		return int64(vv)
	default:
		return 0
	}
}
