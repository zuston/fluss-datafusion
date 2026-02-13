# fluss-datafusion

A lightweight Fluss SQL CLI built on Apache DataFusion.

## Supported

- [x] `CREATE TABLE` (basic columns + `PRIMARY KEY`)
- [x] `INSERT INTO ... VALUES ...`
- [x] `SELECT ... FROM ... WHERE...`
- [x] `SHOW TABLES`
- [x] `SHOW CREATE TABLE <table>`
- [ ] `SELECT ... FROM ... LIMIT 10` for kv/log table

## Example

```sql
CREATE TABLE user (
  id BIGINT NOT NULL,
  name STRING,
  created_at TIMESTAMP,
  PRIMARY KEY (id)
);
INSERT INTO user (id, name, created_at) VALUES (1, 'alice', TIMESTAMP '2026-02-13 10:00:00');
SHOW TABLES;
SHOW CREATE TABLE user;
```
