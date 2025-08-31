# node-red-contrib-sqlite-bulk-insert

Generic high-speed **SQLite INSERT / UPSERT** node for Node-RED with:
- JSONata or msg/flow/global sources
- Column ↔ field mapping (+ transforms)
- Conflict strategies (`IGNORE`, `REPLACE`, **UPSERT** with keys & update columns)
- Transactions (all / chunked / off), optional PRAGMAs
- Optional **RETURNING** of inserted/affected rows with IDs (SQLite ≥ 3.35)

> Built to replace ad-hoc Function nodes for bulk inserts like `INSERT OR IGNORE INTO Train_Position(name) VALUES (?)` while remaining fully declarative and safe.

---

## Install

```bash
npm i node-red-contrib-sqlite-bulk-insert
# or from Node-RED palette manager
````

Requires:

* Node.js ≥ 16
* Node-RED ≥ 3.0
* SQLite library: [`sqlite3`](https://www.npmjs.com/package/sqlite3) (installed as a dependency)

---

## Quick start

1. Add **sqlite-bulk-insert** to your flow.

2. Set **Database path** to a typed value, e.g.:

* type: `msg`
* value: `data.const.paths.databasePath`

3. Set **Table**: `Train_Position`.

4. Choose a **Source**:

* **JSONata** (no extra Function):
  Produces rows `[{ "name": "..." }]` from `Layout` (C\* columns; `;` split; filtered by project/diagram):

  ```jsonata
  (
    $pn := data.const.projectName;
    $dg := data.const.projectDiagram;
    $rows := data.data.input.config.tables.Layout;

    $filtered := $rows[
      $contains($split(Project, ';').$trim(), $pn) and
      $contains(Layout, $dg)
    ];

    $vals := $distinct(
      $filtered.(
        $row := $;
        $keys($row)[$match($, /^C/i)].(
          $split($lookup($row, $), ';').$trim()
        )
      )
    );
    $clean := $vals[$ and $ != "null"];
    $clean.{"name": $}
  )
  ```

* **Or** feed `msg.payload = [{name:"..."}]` from a small Function and set Source to `msg.payload`.

5. Turn **Auto-map** on (it will map column `name` from row key `name`), or add one mapping row manually.

6. Set **Conflict strategy** to **OR IGNORE** (to mirror `INSERT OR IGNORE`).

7. (Optional) **preSQL**:

   ```sql
   CREATE TABLE IF NOT EXISTS Train_Position(
     id   INTEGER PRIMARY KEY,
     name TEXT UNIQUE
   );
   ```

8. (Optional) **Return rows** → Mode: `Inserted`, ID column: `id`, Path: `msg.sqlite.rows`.

Run your flow. The node writes a summary to `msg.sqlite`, and optionally the returned rows to `msg.sqlite.rows`.

---

## Node properties

### Connection

* **Database path** *(typed: str/msg/flow/global/env)* – path to the `.sqlite`/`.db` file.
* **PRAGMAs**

  * `journal_mode = WAL` *(checkbox)*
  * `synchronous` *(OFF/NORMAL/FULL/EXTRA)*
  * Extra PRAGMAs (semicolon-separated), e.g. `temp_store=MEMORY; cache_size=20000`.

### Source

* **Source** *(typed: msg/flow/global/str/jsonata)* – an array of records or a single object (wrapped to array).
* **Table** – SQLite table name (identifier: letters/digits/underscore; not starting with digit).

### Mapping

* **Auto-map** – when on, all keys from the first object row are mapped to columns of the same name.
* **Manual mapping table**

  * **Column** – DB column name.
  * **Source** *(typed)* – where to read the value from the row (`path`, `jsonata`, `msg`, `flow`, `global`, `env`, `str`, `num`, `bool`, `json`).
  * **Transform** – `none | trim | upper | lower | nz | bool01 | number | string`.

### Conflict

* **Strategy** – `none` (INSERT), `ignore` (OR IGNORE), `replace` (OR REPLACE), **`upsert`**.
* **UPSERT keys** – comma list of conflict target columns.
* **UPSERT update columns** – comma list of columns to update on conflict.
* Buttons to fill from mapping columns.

### Transaction

* **Mode** – `all` (single transaction), `chunk` (batch commit), `off`.
* **Chunk size** – rows per commit when in `chunk` mode.
* **Continue on error** – keep processing rows; counts will include errors/skips.
* **preSQL / postSQL** – executed before/after the bulk operation.

### Output

* **Summary output path** *(typed: msg/flow/global)* – where to write the summary (default: `msg.sqlite`).
* **Return rows** *(optional; SQLite ≥ 3.35 for RETURNING)*

  * **Mode** – `none | inserted | affected`.
  * **ID column** – primary key column, e.g. `id` or `rowid`.
  * **Path** *(typed)* – e.g. `msg.sqlite.rows`.

---

## Output shape

**Summary** (default `msg.sqlite`)

```json
{
  "ok": true,
  "table": "Train_Position",
  "counts": { "inserted": 12, "updated": 0, "skipped": 0, "errors": 0, "total": 12 },
  "firstInsertId": 101,
  "lastInsertId": 112,
  "timings": { "msOpen": 3, "msExec": 18, "msTotal": 27 }
}
```

**Returned rows** (if enabled; default `msg.sqlite.rows`)

```json
[
  { "action": "inserted", "id": 101, "data": { "name": "C01-05" } },
  { "action": "inserted", "id": 102, "data": { "name": "C01-06" } }
]
```

> On older SQLite versions without `RETURNING`, the node falls back to `lastID` (for inserts) and key lookups (for upserts) when possible.

---

## Tips

* Use **Auto-map** for simple structures; switch to manual mapping for renames and typed sources.
* Prefer `UPSERT` with precise **keys** and **update columns** for idempotent loads.
* For very large loads, set **chunk size** (e.g., 500–2000) and enable **WAL**.

---

## Changelog

* **0.1.0** – Initial release.

---

## License

[MIT](LICENSE)

---

## Support / Issues

Please open issues or PRs on GitHub.
