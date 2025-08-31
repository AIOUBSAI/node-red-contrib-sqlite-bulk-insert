/**
 * sqlite-bulk-insert.js
 * Generic bulk insert/upssert into SQLite with mapping + JSONata row builder.
 *
 * Requires: npm i sqlite3
 *
 * Node config (saved by editor):
 *  - name
 *  - dbPathType, dbPath
 *  - pragmas: { wal:boolean, sync:string|null, extra:string|null }
 *  - sourceType (typed: msg|flow|global|jsonata|str), source
 *  - table (string)
 *  - autoMap (bool)
 *  - mapping: Array<{ column, srcType, src, transform }>
 *  - conflict: { strategy:'none'|'ignore'|'replace'|'upsert', keys:[], updateCols:[] }
 *  - tx: { mode:'all'|'chunk'|'off', chunkSize:number, continueOnError:boolean, preSQL:string, postSQL:string }
 *  - out: { pathType:'msg|flow|global', path:'sqlite' }
 *  - return: { mode:'none'|'inserted'|'affected', idCol:'id'|'rowid', pathType, path }
 */

module.exports = function(RED){
  const sqlite3 = require("sqlite3");

  // ---------- Utilities ----------
  const isObj = v => v && typeof v === "object" && !Array.isArray(v);

  function qid(id){
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(String(id || ""))) {
      throw new Error(`Invalid identifier: ${id}`);
    }
    return `"${id}"`;
  }

  function parseSqliteVersion(vstr){
    const v = String(vstr || "0.0.0").split(".").map(n=>parseInt(n,10) || 0);
    while (v.length < 3) v.push(0);
    return { major:v[0], minor:v[1], patch:v[2] };
  }

  function versionGte(a,b){
    if (a.major !== b.major) return a.major > b.major;
    if (a.minor !== b.minor) return a.minor > b.minor;
    return a.patch >= b.patch;
  }

  function setTyped(RED, node, msg, type, path, value){
    if (!path) return;
    switch (type) {
      case "flow":   node.context().flow.set(path, value); break;
      case "global": node.context().global.set(path, value); break;
      default:       RED.util.setMessageProperty(msg, path, value, true);
    }
  }

  function getTyped(RED, node, msg, type, value){
    switch (type) {
      case "num": return Number(value);
      case "bool": return !!value;
      case "env": return process.env[String(value)] || "";
      case "msg": return RED.util.getMessageProperty(msg, String(value));
      case "flow": return node.context().flow.get(String(value));
      case "global": return node.context().global.get(String(value));
      case "json": try { return JSON.parse(String(value)); } catch { return undefined; }
      case "jsonata": {
        try {
          const expr = RED.util.prepareJSONataExpression(String(value), node);
          return RED.util.evaluateJSONataExpression(expr, msg);
        } catch { return undefined; }
      }
      case "str":
      default:
        return value;
    }
  }

  // Row-level resolution (can read from row via "path", or use typed sources, or JSONata using msg+row)
  function getForRow(RED, node, msg, row, srcType, src){
    if (srcType === "path") {
      // dot path into the row object
      if (!src) return undefined;
      const parts = String(src).split(".");
      let cur = row;
      for (const k of parts) {
        if (!isObj(cur) && !Array.isArray(cur)) return undefined;
        cur = cur?.[k];
      }
      return cur;
    }
    if (srcType === "jsonata") {
      try {
        const expr = RED.util.prepareJSONataExpression(String(src), node);
        const augmentedMsg = Object.assign({}, msg, { row }); // expose current row as msg.row
        return RED.util.evaluateJSONataExpression(expr, augmentedMsg);
      } catch { return undefined; }
    }
    // fallback to generic typed read
    return getTyped(RED, node, msg, srcType, src);
  }

  function applyTransform(val, transform){
    switch (transform) {
      case "trim": return (val == null) ? val : String(val).trim();
      case "upper": return (val == null) ? val : String(val).toUpperCase();
      case "lower": return (val == null) ? val : String(val).toLowerCase();
      case "nz": { // empty/"NA"/"N/A" => null
        if (val == null) return null;
        const s = String(val).trim();
        if (!s || /^N\/?A$/i.test(s)) return null;
        return val;
      }
      case "bool01": {
        if (val === true || val === 1 || String(val).toLowerCase() === "true") return 1;
        return 0;
      }
      case "number": {
        if (val == null || val === "") return null;
        const n = Number(val);
        return isNaN(n) ? null : n;
      }
      case "string": {
        if (val == null) return null;
        return String(val);
      }
      case "none":
      default:
        return val;
    }
  }

  function buildInsertSQL({ table, cols, strategy, upsertKeys, upsertUpdateCols, idCol, useReturning }){
    const baseCols = cols.map(qid).join(", ");
    const ph = cols.map(()=>"?").join(", ");
    let sql = `INSERT ${strategy==='ignore' ? 'OR IGNORE ' : strategy==='replace' ? 'OR REPLACE ' : ''}INTO ${qid(table)} (${baseCols}) VALUES (${ph})`;
    if (strategy === "upsert" && Array.isArray(upsertKeys) && upsertKeys.length) {
      const updateSet = (Array.isArray(upsertUpdateCols) ? upsertUpdateCols : [])
        .filter(c => cols.includes(c))
        .map(c => `${qid(c)}=excluded.${qid(c)}`)
        .join(", ");
      // If no columns chosen to update, make it a no-op update on a key to satisfy syntax.
      const setClause = updateSet || `${qid(upsertKeys[0])}=${qid(upsertKeys[0])}`;
      sql += ` ON CONFLICT(${upsertKeys.map(qid).join(",")}) DO UPDATE SET ${setClause}`;
    }
    if (useReturning) {
      // Return id + inserted columns so caller can forward the values if needed
      const idSel = idCol ? qid(idCol) : "rowid";
      sql += ` RETURNING ${idSel} AS __id, ${baseCols}`;
    }
    return sql;
  }

  async function detectReturningSupport(db){
    // cache result on db handle (session-local)
    if (db.__supportsReturning !== undefined) return db.__supportsReturning;
    const ver = await new Promise((res)=>db.get("select sqlite_version() v", (e,row)=>res(row?.v || "0.0.0")));
    const v = parseSqliteVersion(ver);
    db.__supportsReturning = versionGte(v, {major:3, minor:35, patch:0});
    return db.__supportsReturning;
  }

  function prepareAsync(db, sql){
    return new Promise((res,rej)=>db.prepare(sql, function(e){ e?rej(e):res(this); }));
  }
  function finalizeAsync(stmt){
    return new Promise((res,rej)=>stmt.finalize(e=>e?rej(e):res()));
  }
  function runAsync(db, sql, params=[]){
    return new Promise((res,rej)=>db.run(sql, params, function(e){ e?rej(e):res(this); }));
  }
  function getAsync(db, sql, params=[]){
    return new Promise((res,rej)=>db.get(sql, params, (e,row)=>e?rej(e):res(row)));
  }
  function allAsync(db, sql, params=[]){
    return new Promise((res,rej)=>db.all(sql, params, (e,rows)=>e?rej(e):res(rows)));
  }

  async function execBatch(RED, node, msg, db, rows, cfg){
    const {
      table, columns, mapRowToParams,
      strategy, upsertKeys, upsertUpdateCols,
      idCol = "id",
      txMode = "all", chunkSize = 500, continueOnError = false,
      preSQL, postSQL,
      returnMode = "none" // 'none'|'inserted'|'affected' (affected includes upserts)
    } = cfg;

    const startTotal = Date.now();
    const counts = { inserted:0, updated:0, skipped:0, errors:0, total: rows.length };
    const returned = [];
    let firstInsertId = null, lastInsertId = null;

    // PRAGMA/sql hooks are executed outside the timing breakdown
    if (preSQL && preSQL.trim()) await runAsync(db, preSQL);

    const supportsReturning = (returnMode !== "none") ? await detectReturningSupport(db) : false;
    const useReturning = (returnMode !== "none") && supportsReturning;

    const sql = buildInsertSQL({
      table,
      cols: columns,
      strategy,
      upsertKeys,
      upsertUpdateCols,
      idCol: idCol || "rowid",
      useReturning
    });

    const doChunk = async (batch) => {
      if (useReturning){
        // With RETURNING we must use get() per row to capture a row back
        for (const r of batch) {
          const params = mapRowToParams(r);
          try{
            const row = await getAsync(db, sql, params);
            if (!row) { counts.skipped++; continue; }
            const ret = { action: 'inserted', id: row.__id, data: {} };
            // reconstruct data columns for caller (exclude __id)
            for (const c of columns) ret.data[c] = row[c];
            // With UPSERT RETURNING returns the affected row as well — classify as 'affected'
            if (strategy === "upsert") ret.action = 'affected';

            if (ret.action === 'inserted') counts.inserted++;
            else if (ret.action === 'affected') counts.updated++;

            lastInsertId = row.__id ?? lastInsertId;
            if (firstInsertId == null && lastInsertId != null) firstInsertId = lastInsertId;

            if (returnMode !== "none") returned.push(ret);
          } catch (e){
            counts.errors++;
            if (!continueOnError) throw e;
          }
        }
      } else {
        // Prepared statement (faster)
        const stmt = await prepareAsync(db, sql);
        for (const r of batch) {
          const params = mapRowToParams(r);
          await new Promise((res,rej)=>stmt.run(params, async function(e){
            if (e){
              counts.errors++;
              if (!continueOnError) return rej(e);
              return res();
            }
            if (this.changes === 0) {
              counts.skipped++;
              return res();
            }
            if (strategy === "upsert" && !this.lastID) {
              // UPDATE branch on older SQLite. Try to get id back using upsert keys if provided.
              counts.updated++;
              if (returnMode === "affected" && Array.isArray(upsertKeys) && upsertKeys.length) {
                try {
                  const m = Object.fromEntries(columns.map((c,i)=>[c, params[i]]));
                  const where = upsertKeys.map(k=>`${qid(k)}=?`).join(" AND ");
                  const vals  = upsertKeys.map(k=>m[k]);
                  const row = await getAsync(db, `SELECT ${qid(idCol)} AS id FROM ${qid(table)} WHERE ${where} LIMIT 1`, vals);
                  if (row && row.id != null) {
                    returned.push({ action:'updated', id: row.id, data: m });
                  }
                } catch {}
              }
              return res();
            }
            // Insert branch
            counts.inserted++;
            lastInsertId = this.lastID ?? lastInsertId;
            if (firstInsertId == null && lastInsertId != null) firstInsertId = lastInsertId;
            if (returnMode !== "none") {
              const m = Object.fromEntries(columns.map((c,i)=>[c, params[i]]));
              returned.push({ action:'inserted', id: this.lastID, data: m });
            }
            res();
          }));
        }
        await finalizeAsync(stmt);
      }
    };

    const startExec = Date.now();

    // Transaction logic
    const runBatches = async () => {
      if (txMode === "off") {
        const batches = [rows];
        for (const b of batches) await doChunk(b);
        return;
      }
      if (txMode === "all") {
        await runAsync(db, "BEGIN");
        try { await doChunk(rows); await runAsync(db, "COMMIT"); }
        catch(e){ try{ await runAsync(db,"ROLLBACK"); }catch{} throw e; }
        return;
      }
      if (txMode === "chunk") {
        const n = Math.max(1, Number(chunkSize)||500);
        for (let i=0; i<rows.length; i+=n){
          const batch = rows.slice(i, i+n);
          await runAsync(db, "BEGIN");
          try { await doChunk(batch); await runAsync(db, "COMMIT"); }
          catch(e){ try{ await runAsync(db,"ROLLBACK"); }catch{} if (!continueOnError) throw e; }
        }
        return;
      }
      // default fallback
      await runAsync(db, "BEGIN");
      try { await doChunk(rows); await runAsync(db, "COMMIT"); }
      catch(e){ try{ await runAsync(db,"ROLLBACK"); }catch{} throw e; }
    };

    await runBatches();

    const msExec = Date.now() - startExec;

    if (postSQL && postSQL.trim()) await runAsync(db, postSQL);

    const msTotal = Date.now() - startTotal;

    return {
      counts,
      returned,
      firstInsertId,
      lastInsertId,
      timings: { msExec, msTotal }
    };
  }

  // ---------- Node definition ----------
  function SqliteBulkInsert(config){
    RED.nodes.createNode(this, config);
    const node = this;

    node.name = config.name || "";

    // Connection
    node.dbPathType = config.dbPathType || "str";
    node.dbPath     = config.dbPath || "";

    node.pragmas = {
      wal: !!(config.pragmas && config.pragmas.wal),
      sync: (config.pragmas && config.pragmas.sync) || "",
      extra: (config.pragmas && config.pragmas.extra) || ""
    };

    // Source
    node.sourceType = config.sourceType || "msg";   // typed (msg/flow/global/jsonata/str)
    node.source     = config.source || "payload";   // path or jsonata
    node.table      = config.table || "";
    node.autoMap    = !!config.autoMap;

    // Mapping array
    node.mapping = Array.isArray(config.mapping) ? config.mapping : []; // [{column, srcType, src, transform}]

    // Conflict
    node.conflict = {
      strategy: (config.conflict && config.conflict.strategy) || "none",
      keys: Array.isArray(config.conflict && config.conflict.keys) ? config.conflict.keys : [],
      updateCols: Array.isArray(config.conflict && config.conflict.updateCols) ? config.conflict.updateCols : []
    };

    // Transaction
    node.tx = {
      mode: (config.tx && config.tx.mode) || "all",
      chunkSize: Number(config.tx && config.tx.chunkSize) || 500,
      continueOnError: !!(config.tx && config.tx.continueOnError),
      preSQL: (config.tx && config.tx.preSQL) || "",
      postSQL: (config.tx && config.tx.postSQL) || ""
    };

    // Output summary
    node.out = {
      pathType: (config.out && config.out.pathType) || "msg",
      path: (config.out && config.out.path) || "sqlite"
    };

    // Return rows
    node.ret = {
      mode: (config.ret && config.ret.mode) || "none", // none|inserted|affected
      idCol: (config.ret && config.ret.idCol) || "id",
      pathType: (config.ret && config.ret.pathType) || "msg",
      path: (config.ret && config.ret.path) || "sqlite.rows"
    };

    node.on("input", async function(msg, send, done){
      try{
        // Resolve DB path
        const dbPath = getTyped(RED, node, msg, node.dbPathType, node.dbPath);
        if (!dbPath || typeof dbPath !== "string") {
          node.status({ fill:"red", shape:"ring", text:"invalid db path" });
          throw new Error("Invalid database path");
        }

        // Resolve source rows (array or single object)
        let records = getTyped(RED, node, msg, node.sourceType, node.source);
        if (records == null && node.sourceType === "msg") {
          // default when blank -> msg.payload
          records = msg.payload;
        }
        if (records == null) records = [];
        if (!Array.isArray(records)) records = [records];

        // Auto-map columns if requested and mapping absent
        let columns = [];
        if (node.autoMap){
          const sample = records.find(r => isObj(r)) || {};
          columns = Object.keys(sample);
        } else {
          columns = node.mapping.map(m => m.column).filter(Boolean);
        }
        if (!columns.length){
          node.status({ fill:"red", shape:"ring", text:"no columns" });
          throw new Error("No columns configured");
        }

        // build mapRowToParams
        const mapping = node.autoMap
          ? columns.map(c => ({ column:c, srcType:"path", src:c, transform:"none" }))
          : node.mapping;

        const table = node.table;
        if (!table) throw new Error("Table name is required");

        // Map function per row
        const mapRowToParams = (row) => {
          return mapping.map(m => {
            const raw = getForRow(RED, node, msg, row, m.srcType || "path", m.src);
            return applyTransform(raw, m.transform || "none");
          });
        };

        // Open DB + PRAGMAs
        const db = new sqlite3.Database(dbPath);
        const startOpen = Date.now();
        try {
          if (node.pragmas.wal) await runAsync(db, "PRAGMA journal_mode=WAL");
          if (node.pragmas.sync) await runAsync(db, `PRAGMA synchronous=${node.pragmas.sync}`);
          if (node.pragmas.extra && node.pragmas.extra.trim()) {
            const lines = node.pragmas.extra.split(";").map(s=>s.trim()).filter(Boolean);
            for (const ln of lines) await runAsync(db, ln);
          }

          // Execute
          const res = await execBatch(
            RED,
            node,
            msg,
            db,
            records,
            {
              table,
              columns,
              mapRowToParams,
              strategy: node.conflict.strategy,
              upsertKeys: node.conflict.keys,
              upsertUpdateCols: node.conflict.updateCols,
              idCol: node.ret.idCol || "id",
              txMode: node.tx.mode,
              chunkSize: node.tx.chunkSize,
              continueOnError: node.tx.continueOnError,
              preSQL: node.tx.preSQL,
              postSQL: node.tx.postSQL,
              returnMode: node.ret.mode
            }
          );

          // Build summary
          const summary = {
            ok: res.counts.errors === 0,
            table,
            counts: res.counts,
            firstInsertId: res.firstInsertId,
            lastInsertId: res.lastInsertId,
            timings: Object.assign({ msOpen: (Date.now() - startOpen) }, res.timings)
          };

          // Set outputs
          setTyped(RED, node, msg, node.out.pathType, node.out.path, summary);
          if (node.ret.mode !== "none") {
            setTyped(RED, node, msg, node.ret.pathType, node.ret.path, res.returned);
          }

          // Status bubble
          const s = res.counts;
          const worst = s.errors ? "red" : (s.updated ? "yellow" : "green");
          node.status({ fill: worst, shape:"dot", text: `I:${s.inserted} U:${s.updated} S:${s.skipped} E:${s.errors}` });

          send(msg);
          done();
        } catch(err){
          node.status({ fill:"red", shape:"ring", text:"insert error" });
          done(err);
        } finally {
          try { db.close(); } catch {}
        }
      } catch (e){
        node.status({ fill:"red", shape:"ring", text:e.message });
        done(e);
      }
    });
  }

  RED.nodes.registerType("sqlite-bulk-insert", SqliteBulkInsert);
};
