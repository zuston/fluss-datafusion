# fluss-datafusion

DataFusion integration project for Apache Fluss, providing an interactive SQL CLI tool.

## Architecture Design

```
┌─────────────────────────────────────────────────────────────┐
│                         CLI Layer                          │
│                   (cli.rs - Interactive REPL)            │
├─────────────────────────────────────────────────────────────┤
│                      SQL Extension Layer                   │
│           (sql/ - Dialect, Rewriter, information_schema)   │
├─────────────────────────────────────────────────────────────┤
│                      Catalog Layer                         │
│        (catalog/ - CatalogProvider/SchemaProvider)         │
├─────────────────────────────────────────────────────────────┤
│                      Provider Layer                        │
│       (provider/ - TableProvider/ExecutionPlan)          │
├─────────────────────────────────────────────────────────────┤
│                      Fluss Client                          │
│              (fluss-rs - Fluss cluster communication)      │
└─────────────────────────────────────────────────────────────┘
```

## Supported SQL Commands

### Standard SQL
- [x] `CREATE TABLE` (basic column definition + `PRIMARY KEY`)
- [x] `INSERT INTO ... VALUES ...`
- [x] `SELECT ... FROM ... WHERE ...`

### Fluss Extension SQL

#### Table Management
- [x] `SHOW TABLES [FROM|IN database]` - List tables in database
- [x] `SHOW CREATE TABLE <table>` - Show CREATE TABLE statement
- [x] `DESCRIBE <table>` / `DESC <table>` - Show table structure
- [x] `SHOW DATABASES` / `SHOW SCHEMAS` - List all databases

#### Partitions and Buckets (New!)
- [x] `SHOW PARTITIONS <table>` - Show table partition info (uses Fluss `list_partition_infos` API)
- [x] `SHOW BUCKETS <table>` - Show table bucket info (supports partitioned tables)
- [x] `SHOW [TABLE] OPTIONS <table>` - Show table configuration options

### TODO
- [ ] `SELECT ... FROM ... LIMIT 10` for kv/log tables
- [ ] Support timestamp data type

## Usage Examples

```sql
-- Create table
CREATE TABLE user (
  id BIGINT NOT NULL,
  name STRING,
  PRIMARY KEY (id)
);

-- Insert data
INSERT INTO user (id, name) VALUES (1, 'alice');

-- Query data
SELECT * FROM user WHERE id = 1;

-- List tables
SHOW TABLES;
SHOW TABLES FROM other_db;

-- Show table structure
DESCRIBE user;

-- Show CREATE TABLE statement
SHOW CREATE TABLE user;

-- Show partition info (partitioned tables)
SHOW PARTITIONS events;
-- Output: partition_id | partition_name | partition_qualified_name | num_buckets
--         1            | 2024-01-15$US  | date=2024-01-15/region=US | 4

-- Show bucket info (supports partitioned tables)
SHOW BUCKETS events;
-- Output: table_schema | table_name | partition_id | bucket_id | bucket_key | row_count
--         fluss        | events     | 1            | 0         | user_id    | NULL
--         fluss        | events     | 1            | 1         | user_id    | NULL
--         ...

-- Show table options
SHOW OPTIONS user;
SHOW TABLE OPTIONS user;

-- List all databases
SHOW DATABASES;
```

## CLI Meta-commands

| Command | Description |
|---------|-------------|
| `\dt` | List tables in current database (equivalent to `SHOW TABLES`) |
| `\?` / `help` | Show help information |
| `\q` / `quit` / `exit` | Exit CLI |

## SQL Rewriting Mechanism

This project uses SQL rewriting to support Fluss-specific SQL commands:

```rust
// SHOW PARTITIONS my_table
// is rewritten as:
// SELECT partition_id, partition_values, num_buckets
// FROM information_schema.partitions
// WHERE table_schema = 'current_db' AND table_name = 'my_table'
```

Benefits of this design:
- **Normalized Extension**: All Fluss-specific commands are uniformly converted to standard SQL
- **Easy to Extend**: Add new commands by adding new rewrite rules
- **DataFusion Compatible**: No need to modify DataFusion core code

## Information Schema Virtual Tables

The extended `information_schema` contains the following virtual tables:

| Virtual Table | Purpose | Corresponding SQL Command | Notes |
|---------------|---------|---------------------------|-------|
| `tables` | List all tables | `SHOW TABLES` | |
| `table_ddl` | CREATE TABLE statements | `SHOW CREATE TABLE` | |
| `columns` | Column info | `DESCRIBE` | |
| `partitions` | Partition info | `SHOW PARTITIONS` | Uses Fluss `list_partition_infos` API |
| `buckets` | Bucket info | `SHOW BUCKETS` | Supports partitioned tables |
| `table_options` | Table config | `SHOW OPTIONS` | |
| `table_stats` | Table statistics | (internal use) | |

## Startup Options

```bash
# Interactive mode (default)
fluss-datafusion --bootstrap-server 127.0.0.1:9123 --database fluss

# Non-interactive mode (execute single SQL)
fluss-datafusion -e "SHOW TABLES"
```

## Development Notes

### Adding New SHOW Commands

1. Add parsing function in `src/sql/rewriter.rs`
2. Add corresponding `ShowCommand` variant in `src/sql/show.rs`
3. Update `information_schema` virtual table in `src/catalog/schema.rs`
4. Update command list in CLI help information

### Example: Adding `SHOW TABLE STATUS`

```rust
// src/sql/rewriter.rs
pub fn parse_show_table_status(sql: &str, current_db: &str) -> Option<(String, String)> {
    // Parsing logic...
}

// src/catalog/schema.rs - Add new virtual table or extend existing
async fn build_table_stats_table(&self) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
    // Implementation...
}
```
