# DuckDB-Manifold Extension

A DuckDB extension for querying ManifoldDB graph databases directly via SQL.

## What It Does

Query ManifoldDB's redb storage directly from DuckDB - no data export, no ETL. Point DuckDB at your `.redb` file and run SQL.

## Installation

### Building from Source

```shell
# Clone with submodules
git clone --recurse-submodules https://github.com/tomWhiting/duckdb-manifold.git
cd duckdb-manifold

# Configure (sets up Python venv with DuckDB)
make configure

# Build
make debug    # or: make release
```

### Dependencies

- Rust toolchain
- Python 3
- Make
- Git

## Usage

```python
import duckdb

conn = duckdb.connect(config={'allow_unsigned_extensions': True})
conn.execute("LOAD 'build/debug/duckdb_manifold.duckdb_extension'")

# Query entities
result = conn.execute("SELECT * FROM manifold_entities('/path/to/database.redb')")
for row in result.fetchall():
    print(row)
```

### Query Entities

```sql
SELECT * FROM manifold_entities('/path/to/database.redb');
```

Returns:
- `id` - Entity ID (VARCHAR)
- `labels` - JSON array of labels (VARCHAR)
- `prop_*` - One column per discovered property

### Query Edges

```sql
SELECT * FROM manifold_edges('/path/to/database.redb');
```

Returns:
- `id` - Edge ID (VARCHAR)
- `source` - Source entity ID (VARCHAR)
- `target` - Target entity ID (VARCHAR)
- `edge_type` - Relationship type (VARCHAR)
- `prop_*` - One column per discovered property

### Filter, Aggregate, Join

Full DuckDB SQL works:

```sql
-- Filter entities
SELECT prop_name, CAST(prop_age AS INTEGER) as age
FROM manifold_entities('/path/to/db.redb')
WHERE prop_age != '' AND CAST(prop_age AS INTEGER) > 25;

-- Aggregate edges
SELECT edge_type, COUNT(*) as count
FROM manifold_edges('/path/to/db.redb')
GROUP BY edge_type;

-- Join entities and edges
SELECT e1.prop_name as person, edge.edge_type, e2.prop_name as target
FROM manifold_entities('/path/to/db.redb') e1
JOIN manifold_edges('/path/to/db.redb') edge ON e1.id = edge.source
JOIN manifold_entities('/path/to/db.redb') e2 ON edge.target = e2.id;
```

## How It Works

- **Dynamic schema discovery**: Samples entities at bind time to discover property columns
- **Cursor-based streaming**: Reads in batches of 1024 for efficiency
- **Shared engine cache**: Multiple queries share the same database connection
- **All columns VARCHAR**: DuckDB casts as needed in queries

## Testing

```shell
# Run integration test
cargo run --bin integration_test
```

## Target DuckDB Version

v1.4.3

## License

MIT
