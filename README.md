# fluss-datafusion

A lightweight Fluss SQL CLI built on Apache DataFusion.

## Supported

- [x] `CREATE TABLE` (basic columns + `PRIMARY KEY`)
- [x] `INSERT INTO ... VALUES ...`
- [x] `SELECT ... FROM ... WHERE...`
- [x] `SHOW TABLES`
- [x] `SHOW CREATE TABLE <table>`
- [ ] `SELECT ... FROM ... LIMIT 10` for kv/log table
- [ ] add support of timestamp data type

## Example

```sql
CREATE TABLE user (
  id BIGINT NOT NULL,
  name STRING,
  PRIMARY KEY (id)
);
INSERT INTO user (id, name) VALUES (1, 'alice');
SELECT * FROM user WHERE id = 1;
SHOW TABLES;
SHOW CREATE TABLE user;
```
