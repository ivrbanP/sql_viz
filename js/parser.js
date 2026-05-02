/**
 * SQL Parser - Extracts table relationships from SQL queries.
 * Handles: SELECT, INSERT, CREATE VIEW, UPDATE, DELETE, CTEs, subqueries.
 */
class SQLParser {
  constructor() {
    this.subqueryCount = 0;
    this.aliasMap = new Map(); // alias (lower) -> table name (lower)
    this.dialect = 'postgresql';  // postgresql | oracle | mysql | mssql
  }

  parse(sql, dialect) {
    this.subqueryCount = 0;
    this.aliasMap = new Map();
    this.dialect = (dialect || 'postgresql').toLowerCase();

    sql = this.removeComments(sql);
    const statements = this.splitStatements(sql);
    const nodeMap = new Map();
    const edges = [];

    for (const stmt of statements) {
      const trimmed = stmt.trim();
      if (trimmed) {
        this.analyzeStatement(trimmed, nodeMap, edges);
      }
    }

    // Post-process: scan full SQL for WHERE-clause join conditions
    this.analyzeWhereConditions(sql, nodeMap, edges);

    // Post-process: extract column references from the SQL
    this.extractColumns(sql, nodeMap);

    // Post-process: extract WHERE filter conditions per table
    this.extractWhereFilters(sql, nodeMap);

    // Post-process: extract GROUP BY columns
    this.extractGroupByColumns(sql, nodeMap);

    return {
      nodes: Array.from(nodeMap.values()),
      edges: this.deduplicateEdges(edges)
    };
  }

  // ── Preprocessing ──────────────────────────────────────────────

  removeComments(sql) {
    return sql
      .replace(/--[^\n]*/g, '')
      .replace(/\/\*[\s\S]*?\*\//g, '');
  }

  splitStatements(sql) {
    const stmts = [];
    let current = '';
    let inQ = false, qChar = '';
    let depth = 0;

    for (let i = 0; i < sql.length; i++) {
      const ch = sql[i];
      if (inQ) {
        current += ch;
        if (ch === qChar && sql[i + 1] === qChar) { current += qChar; i++; }
        else if (ch === qChar) inQ = false;
        continue;
      }
      if (ch === "'" || ch === '"' || ch === '`') { inQ = true; qChar = ch; current += ch; }
      else if (ch === '(') { depth++; current += ch; }
      else if (ch === ')') { depth = Math.max(0, depth - 1); current += ch; }
      else if (ch === ';' && depth === 0) { if (current.trim()) stmts.push(current.trim()); current = ''; }
      else current += ch;
    }
    if (current.trim()) stmts.push(current.trim());
    return stmts;
  }

  // ── Tokenizer ──────────────────────────────────────────────────

  tokenize(sql) {
    const tokens = [];
    let i = 0;
    const len = sql.length;

    while (i < len) {
      if (/\s/.test(sql[i])) { i++; continue; }

      // Single-quoted string
      if (sql[i] === "'") {
        let j = i + 1;
        while (j < len) {
          if (sql[j] === "'" && sql[j + 1] === "'") j += 2;
          else if (sql[j] === "'") { j++; break; }
          else j++;
        }
        tokens.push({ t: 'S', v: sql.slice(i, j), u: sql.slice(i, j).toUpperCase() });
        i = j; continue;
      }

      // Quoted identifier
      if (sql[i] === '"' || sql[i] === '`') {
        const q = sql[i]; let j = i + 1;
        while (j < len && sql[j] !== q) j++;
        const val = sql.slice(i + 1, j);
        j++;
        tokens.push({ t: 'I', v: val, u: val.toUpperCase() });
        i = j; continue;
      }

      // Bracket identifier [name]
      if (sql[i] === '[') {
        let j = i + 1;
        while (j < len && sql[j] !== ']') j++;
        const val = sql.slice(i + 1, j);
        j++;
        tokens.push({ t: 'I', v: val, u: val.toUpperCase() });
        i = j; continue;
      }

      // Word
      if (/[a-zA-Z_]/.test(sql[i])) {
        let j = i;
        while (j < len && /[a-zA-Z0-9_]/.test(sql[j])) j++;
        const w = sql.slice(i, j);
        tokens.push({ t: 'W', v: w, u: w.toUpperCase() });
        i = j; continue;
      }

      // Number
      if (/\d/.test(sql[i])) {
        let j = i;
        while (j < len && /[\d.eE]/.test(sql[j])) j++;
        tokens.push({ t: 'N', v: sql.slice(i, j), u: '' });
        i = j; continue;
      }

      // Multi-character operators: ::, !=, <>, <=, >=
      if (i + 1 < len) {
        const two = sql[i] + sql[i + 1];
        if (two === '::' || two === '!=' || two === '<>' || two === '<=' || two === '>=') {
          tokens.push({ t: 'P', v: two, u: two });
          i += 2; continue;
        }
      }

      // Symbol
      tokens.push({ t: 'P', v: sql[i], u: sql[i] });
      i++;
    }
    return tokens;
  }

  // ── Node helpers ───────────────────────────────────────────────

  getOrCreateNode(nodeMap, rawName, type) {
    const name = this.cleanName(rawName);
    const key = name.toLowerCase();
    if (!nodeMap.has(key)) {
      nodeMap.set(key, { id: key, name: name, type: type || 'table', columns: [], filters: [] });
    }
    const node = nodeMap.get(key);
    // Upgrade type if more specific
    if (type && type !== 'table' && node.type === 'table') node.type = type;
    return node;
  }

  cleanName(name) {
    return name.replace(/^["'`[\]]+|["'`[\]]+$/g, '');
  }

  addAlias(alias, tableName) {
    if (alias) {
      this.aliasMap.set(alias.toLowerCase(), tableName.toLowerCase());
    }
  }

  resolveAlias(name) {
    const lower = name.toLowerCase();
    return this.aliasMap.get(lower) || lower;
  }

  // ── Statement dispatch ─────────────────────────────────────────

  analyzeStatement(sql, nodeMap, edges) {
    const tokens = this.tokenize(sql);
    if (tokens.length === 0) return;

    const first = tokens[0].u;

    if (first === 'WITH') {
      this.analyzeCTE(tokens, nodeMap, edges);
    } else if (first === 'SELECT') {
      const resultNode = this.getOrCreateNode(nodeMap, 'Query Result', 'result');
      resultNode.columns = this.extractSelectListColumns(tokens, 0, tokens.length);
      this.analyzeSelect(tokens, 0, tokens.length, nodeMap, edges, resultNode.id);
    } else if (first === 'INSERT') {
      this.analyzeInsert(tokens, nodeMap, edges);
    } else if (first === 'CREATE') {
      this.analyzeCreate(tokens, nodeMap, edges);
    } else if (first === 'UPDATE') {
      this.analyzeUpdate(tokens, nodeMap, edges);
    } else if (first === 'DELETE') {
      this.analyzeDelete(tokens, nodeMap, edges);
    } else if (first === 'MERGE') {
      this.analyzeMerge(tokens, nodeMap, edges);
    }
  }

  // ── SELECT ─────────────────────────────────────────────────────

  analyzeSelect(tokens, start, end, nodeMap, edges, targetId) {
    // Handle UNION/EXCEPT/INTERSECT by splitting at depth 0
    const parts = this.splitUnion(tokens, start, end);
    const allTables = [];

    for (const part of parts) {
      const tables = this.analyzeSelectPart(tokens, part.start, part.end, nodeMap, edges, targetId);
      allTables.push(...tables);
    }
    return allTables;
  }

  extractSelectListColumns(tokens, start, end) {
    // Skip SELECT [DISTINCT|ALL] [TOP N]
    let i = start;
    if (i < end && tokens[i].u === 'SELECT') i++;
    if (i < end && (tokens[i].u === 'DISTINCT' || tokens[i].u === 'ALL')) i++;
    // MSSQL: TOP N [PERCENT] [WITH TIES]
    if (i < end && tokens[i].u === 'TOP') {
      i++;
      if (i < end && tokens[i].v === '(') { i = this.findCloseParen(tokens, i) + 1; }
      else if (i < end && tokens[i].t === 'N') { i++; }
      if (i < end && tokens[i].u === 'PERCENT') i++;
      if (i + 1 < end && tokens[i].u === 'WITH' && tokens[i + 1].u === 'TIES') i += 2;
    }

    // Find FROM at depth 0 to know where the select list ends
    let depth = 0;
    let fromPos = end;
    for (let j = i; j < end; j++) {
      if (tokens[j].v === '(') depth++;
      if (tokens[j].v === ')') depth--;
      if (depth === 0 && tokens[j].u === 'FROM') { fromPos = j; break; }
    }

    // Split select list on commas at depth 0
    const cols = [];
    depth = 0;
    let itemStart = i;
    for (let j = i; j <= fromPos; j++) {
      if (j < fromPos) {
        if (tokens[j].v === '(') depth++;
        if (tokens[j].v === ')') depth--;
      }
      if ((depth === 0 && j < fromPos && tokens[j].v === ',') || j === fromPos) {
        // Extract column name: last AS alias, or last word token
        const itemTokens = tokens.slice(itemStart, j);
        const name = this.resolveSelectItemName(itemTokens);
        if (name && name !== '*') cols.push(name);
        itemStart = j + 1;
      }
    }
    return cols;
  }

  resolveSelectItemName(itemTokens) {
    if (itemTokens.length === 0) return null;

    // If there's an AS keyword, the alias follows it
    for (let i = itemTokens.length - 1; i >= 0; i--) {
      if (itemTokens[i].u === 'AS' && i + 1 < itemTokens.length) {
        return itemTokens[i + 1].v;
      }
    }

    // No AS — use the last word token (handles alias.col -> col)
    for (let i = itemTokens.length - 1; i >= 0; i--) {
      if (itemTokens[i].t === 'W' && !this.isKeyword(itemTokens[i].u)) {
        return itemTokens[i].v;
      }
    }

    return null;
  }

  splitUnion(tokens, start, end) {
    const parts = [];
    let depth = 0;
    let partStart = start;

    for (let i = start; i < end; i++) {
      if (tokens[i].v === '(') depth++;
      if (tokens[i].v === ')') depth--;
      if (depth === 0 && (tokens[i].u === 'UNION' || tokens[i].u === 'EXCEPT' || tokens[i].u === 'INTERSECT' || tokens[i].u === 'MINUS')) {
        parts.push({ start: partStart, end: i });
        // Skip ALL
        i++;
        if (i < end && tokens[i].u === 'ALL') i++;
        partStart = i;
      }
    }
    parts.push({ start: partStart, end: end });
    return parts;
  }

  analyzeSelectPart(tokens, start, end, nodeMap, edges, targetId) {
    const sourceTables = [];

    // Find FROM at depth 0
    let depth = 0;
    let fromPos = -1;

    for (let i = start; i < end; i++) {
      if (tokens[i].v === '(') depth++;
      if (tokens[i].v === ')') depth--;
      if (depth === 0 && tokens[i].u === 'FROM') {
        fromPos = i;
        break;
      }
    }

    if (fromPos === -1) return sourceTables;

    // Find FROM clause end
    const fromEnd = this.findFromClauseEnd(tokens, fromPos + 1, end);

    // Parse FROM clause
    let i = fromPos + 1;
    let prevTable = null;
    const sourceTableIds = new Set();
    // Track which tables already have an edge to targetId (to avoid duplicates)
    const tablesWithTargetEdge = new Set();

    while (i < fromEnd) {
      if (tokens[i].v === ',') { i++; continue; }

      // Check for JOIN keywords
      const joinMatch = this.matchJoin(tokens, i);
      if (joinMatch.matched) {
        i = joinMatch.nextPos;
        const result = this.readTableOrSubquery(tokens, i, fromEnd, nodeMap, edges);
        if (result) {
          sourceTables.push(result.node);
          i = result.nextPos;

          // Read ON/USING condition
          const cond = this.readJoinCondition(tokens, i, fromEnd);
          i = cond.nextPos;

          if (prevTable) {
            if (targetId) {
              // Hub model: all joined tables point directly to the result/target node
              if (!tablesWithTargetEdge.has(prevTable.id)) {
                edges.push({
                  source: prevTable.id,
                  target: targetId,
                  type: 'join',
                  label: '',
                  condition: ''
                });
                tablesWithTargetEdge.add(prevTable.id);
              }
              edges.push({
                source: result.node.id,
                target: targetId,
                type: 'join',
                label: joinMatch.type,
                condition: cond.text
              });
              tablesWithTargetEdge.add(result.node.id);
            } else {
              // Chain model (no result node): prevTable -> joinedTable
              let actualSource = prevTable.id;
              if (cond.text) {
                const refs = [...cond.text.matchAll(/\b(\w+)\s*\./g)].map(m => this.resolveAlias(m[1]));
                const newId = result.node.id.toLowerCase();
                const sourceRef = refs.find(r => r !== newId && sourceTableIds.has(r));
                if (sourceRef) actualSource = sourceRef;
              }
              edges.push({
                source: actualSource,
                target: result.node.id,
                type: 'join',
                label: joinMatch.type,
                condition: cond.text
              });
            }
          }
          sourceTableIds.add(result.node.id.toLowerCase());
          prevTable = result.node;
        }
        continue;
      }

      // Regular table or subquery
      const result = this.readTableOrSubquery(tokens, i, fromEnd, nodeMap, edges);
      if (result) {
        sourceTables.push(result.node);
        sourceTableIds.add(result.node.id.toLowerCase());

        // Track last table for possible subsequent JOIN
        prevTable = result.node;
        i = result.nextPos;
      } else {
        i++;
      }
    }

    // Also scan for subqueries in WHERE/HAVING/SELECT clauses
    this.scanSubqueries(tokens, start, fromPos, nodeMap, edges);
    this.scanSubqueries(tokens, fromEnd, end, nodeMap, edges);

    // Data flow edges to target for tables not already connected via join edges
    if (targetId) {
      for (const table of sourceTables) {
        if (!tablesWithTargetEdge.has(table.id)) {
          edges.push({
            source: table.id,
            target: targetId,
            type: 'data_flow',
            label: '',
            condition: ''
          });
        }
      }
    }

    return sourceTables;
  }

  readTableOrSubquery(tokens, pos, limit, nodeMap, edges) {
    if (pos >= limit) return null;

    // Subquery: ( SELECT ... ) [AS] alias
    if (tokens[pos].v === '(' && this.isSubquery(tokens, pos)) {
      const closeP = this.findCloseParen(tokens, pos);
      const subName = 'subquery_' + (++this.subqueryCount);
      const subNode = this.getOrCreateNode(nodeMap, subName, 'subquery');

      // Recursively analyze — pass subNode.id so inner tables get data_flow edges
      this.analyzeSelect(tokens, pos + 1, closeP, nodeMap, edges, subNode.id);

      let nextPos = closeP + 1;

      // Read alias
      if (nextPos < limit && tokens[nextPos].u === 'AS') nextPos++;
      if (nextPos < limit && this.isIdentifier(tokens[nextPos])) {
        const aliasName = tokens[nextPos].v;
        // Rename the subquery node — update edges that reference old id
        const oldId = subNode.id;
        nodeMap.delete(oldId);
        subNode.name = aliasName;
        subNode.id = aliasName.toLowerCase();
        nodeMap.set(subNode.id, subNode);
        this.addAlias(aliasName, subNode.id);
        // Patch edges that used the temporary subquery id
        for (const edge of edges) {
          if (edge.source === oldId) edge.source = subNode.id;
          if (edge.target === oldId) edge.target = subNode.id;
        }
        nextPos++;
      }

      return { node: subNode, nextPos };
    }

    // LATERAL subquery
    if (tokens[pos].u === 'LATERAL') {
      return this.readTableOrSubquery(tokens, pos + 1, limit, nodeMap, edges);
    }

    // Regular table: [schema.]table [AS] alias
    const ref = this.readTableRef(tokens, pos);
    if (ref) {
      // Oracle: skip DUAL pseudo-table
      if (this.dialect === 'oracle' && ref.name.toLowerCase() === 'dual') {
        return null;
      }

      const node = this.getOrCreateNode(nodeMap, ref.name, 'table');
      if (ref.alias) {
        this.addAlias(ref.alias, ref.name);
      }

      let nextPos = ref.nextPos;

      // MSSQL: skip WITH (NOLOCK) / WITH (READUNCOMMITTED) etc.
      if (this.dialect === 'mssql' && nextPos < limit && tokens[nextPos].u === 'WITH' &&
          nextPos + 1 < limit && tokens[nextPos + 1].v === '(') {
        const cp = this.findCloseParen(tokens, nextPos + 1);
        nextPos = cp + 1;
      }

      // MySQL: skip USE INDEX / FORCE INDEX / IGNORE INDEX hints
      if (this.dialect === 'mysql' && nextPos < limit &&
          (tokens[nextPos].u === 'USE' || tokens[nextPos].u === 'FORCE' || tokens[nextPos].u === 'IGNORE') &&
          nextPos + 1 < limit && tokens[nextPos + 1].u === 'INDEX') {
        nextPos += 2;
        if (nextPos < limit && tokens[nextPos].v === '(') {
          nextPos = this.findCloseParen(tokens, nextPos) + 1;
        }
      }

      return { node, nextPos };
    }

    return null;
  }

  readTableRef(tokens, pos) {
    if (pos >= tokens.length) return null;
    if (!this.isIdentifier(tokens[pos])) return null;

    let name = tokens[pos].v;
    let i = pos + 1;

    // schema.table or catalog.schema.table
    while (i + 1 < tokens.length && tokens[i].v === '.' && this.isIdentifier(tokens[i + 1])) {
      name += '.' + tokens[i + 1].v;
      i += 2;
    }

    // Alias
    let alias = '';
    if (i < tokens.length) {
      if (tokens[i].u === 'AS' && i + 1 < tokens.length && this.isIdentifier(tokens[i + 1]) && !this.isKeyword(tokens[i + 1].u)) {
        alias = tokens[i + 1].v;
        i += 2;
      } else if (this.isIdentifier(tokens[i]) && !this.isKeyword(tokens[i].u)) {
        alias = tokens[i].v;
        i++;
      }
    }

    return { name, alias, nextPos: i };
  }

  matchJoin(tokens, pos) {
    const modifiers = new Set(['INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS', 'NATURAL']);
    let i = pos;
    let type = '';

    // MSSQL: CROSS APPLY / OUTER APPLY
    if (this.dialect === 'mssql') {
      if (i + 1 < tokens.length && tokens[i].u === 'CROSS' && tokens[i + 1].u === 'APPLY') {
        return { matched: true, type: 'CROSS APPLY', nextPos: i + 2 };
      }
      if (i + 1 < tokens.length && tokens[i].u === 'OUTER' && tokens[i + 1].u === 'APPLY') {
        return { matched: true, type: 'OUTER APPLY', nextPos: i + 2 };
      }
    }

    while (i < tokens.length && modifiers.has(tokens[i].u)) {
      type += tokens[i].u + ' ';
      i++;
      if (i < tokens.length && tokens[i].u === 'OUTER') {
        type += 'OUTER ';
        i++;
      }
    }

    if (i < tokens.length && tokens[i].u === 'JOIN') {
      type = (type + 'JOIN').trim();
      return { matched: true, type, nextPos: i + 1 };
    }

    // MySQL: STRAIGHT_JOIN
    if (this.dialect === 'mysql' && pos < tokens.length && tokens[pos].u === 'STRAIGHT_JOIN') {
      return { matched: true, type: 'STRAIGHT_JOIN', nextPos: pos + 1 };
    }

    if (pos < tokens.length && tokens[pos].u === 'JOIN') {
      return { matched: true, type: 'JOIN', nextPos: pos + 1 };
    }

    return { matched: false };
  }

  readJoinCondition(tokens, pos, limit) {
    let i = pos;

    if (i < limit && tokens[i].u === 'ON') {
      i++;
      const condStart = i;
      let depth = 0;
      while (i < limit) {
        if (tokens[i].v === '(') depth++;
        if (tokens[i].v === ')') depth--;
        if (depth === 0) {
          if (tokens[i].v === ',') break;
          const jm = this.matchJoin(tokens, i);
          if (jm.matched) break;
        }
        i++;
      }
      return { text: tokens.slice(condStart, i).map(t => t.v).join(' '), nextPos: i };
    }

    if (i < limit && tokens[i].u === 'USING') {
      i++;
      if (i < limit && tokens[i].v === '(') {
        const cp = this.findCloseParen(tokens, i);
        const text = 'USING ' + tokens.slice(i, cp + 1).map(t => t.v).join(' ');
        return { text, nextPos: cp + 1 };
      }
    }

    return { text: '', nextPos: i };
  }

  findFromClauseEnd(tokens, start, end) {
    let depth = 0;
    const endKw = new Set(['WHERE', 'GROUP', 'HAVING', 'ORDER', 'LIMIT', 'UNION', 'EXCEPT',
      'INTERSECT', 'FETCH', 'OFFSET', 'WINDOW', 'FOR', 'INTO', 'MINUS']);
    // Oracle: CONNECT BY, START WITH also end the FROM clause
    if (this.dialect === 'oracle') {
      endKw.add('CONNECT');
      endKw.add('START');
    }
    // MSSQL: OPTION clause
    if (this.dialect === 'mssql') {
      endKw.add('OPTION');
    }
    for (let i = start; i < end; i++) {
      if (tokens[i].v === '(') depth++;
      if (tokens[i].v === ')') depth--;
      if (depth === 0 && tokens[i].t === 'W' && endKw.has(tokens[i].u)) {
        return i;
      }
    }
    return end;
  }

  // ── INSERT ─────────────────────────────────────────────────────

  analyzeInsert(tokens, nodeMap, edges) {
    let i = 0;
    if (tokens[i].u === 'INSERT') i++;
    if (i < tokens.length && tokens[i].u === 'INTO') i++;

    const ref = this.readTableRef(tokens, i);
    if (!ref) return;

    const targetNode = this.getOrCreateNode(nodeMap, ref.name, 'table');
    i = ref.nextPos;

    // Read column list
    if (i < tokens.length && tokens[i].v === '(') {
      const cp = this.findCloseParen(tokens, i);
      const cols = [];
      for (let j = i + 1; j < cp; j++) {
        if (this.isIdentifier(tokens[j])) cols.push(tokens[j].v);
      }
      if (cols.length > 0) targetNode.columns = cols;
      i = cp + 1;
    }

    // Skip OVERRIDING keyword etc
    while (i < tokens.length && tokens[i].u !== 'SELECT' && tokens[i].u !== 'VALUES' && tokens[i].u !== 'WITH' && tokens[i].v !== '(') {
      i++;
    }

    // SELECT or WITH
    if (i < tokens.length && (tokens[i].u === 'SELECT' || tokens[i].u === 'WITH')) {
      if (tokens[i].u === 'WITH') {
        this.analyzeCTE(tokens.slice(i), nodeMap, edges, targetNode.id);
      } else {
        this.analyzeSelect(tokens, i, tokens.length, nodeMap, edges, targetNode.id);
      }
    }
  }

  // ── CREATE VIEW / TABLE AS ────────────────────────────────────

  analyzeCreate(tokens, nodeMap, edges) {
    let i = 1; // Skip CREATE
    if (i < tokens.length && tokens[i].u === 'OR') { i++; if (i < tokens.length && tokens[i].u === 'REPLACE') i++; }
    if (i < tokens.length && tokens[i].u === 'MATERIALIZED') i++;

    if (i < tokens.length && tokens[i].u === 'VIEW') {
      i++;
      const ref = this.readTableRef(tokens, i);
      if (!ref) return;
      const viewNode = this.getOrCreateNode(nodeMap, ref.name, 'view');
      i = ref.nextPos;

      // Skip column list
      if (i < tokens.length && tokens[i].v === '(') {
        const cp = this.findCloseParen(tokens, i);
        i = cp + 1;
      }

      while (i < tokens.length && tokens[i].u !== 'AS') i++;
      if (i < tokens.length) i++;

      if (i < tokens.length && (tokens[i].u === 'SELECT' || tokens[i].u === 'WITH')) {
        if (tokens[i].u === 'WITH') {
          // CTE-backed view: columns come from the final SELECT inside the CTE
          // analyzeCTE will route output to viewNode.id; extract columns from the
          // main SELECT that follows the last CTE body.
          const cteTokens = tokens.slice(i);
          // Find the main SELECT position inside the CTE token slice
          const mainSelectIdx = this._findCTEMainSelectIndex(cteTokens);
          if (mainSelectIdx !== -1) {
            viewNode.columns = this.extractSelectListColumns(cteTokens, mainSelectIdx, cteTokens.length);
          }
          this.analyzeCTE(cteTokens, nodeMap, edges, viewNode.id);
        } else {
          viewNode.columns = this.extractSelectListColumns(tokens, i, tokens.length);
          this.analyzeSelect(tokens, i, tokens.length, nodeMap, edges, viewNode.id);
        }
      }
    } else if (i < tokens.length && tokens[i].u === 'TABLE') {
      i++;
      if (i < tokens.length && tokens[i].u === 'IF') { while (i < tokens.length && tokens[i].u !== 'EXISTS') i++; i++; }
      const ref = this.readTableRef(tokens, i);
      if (!ref) return;
      const tableNode = this.getOrCreateNode(nodeMap, ref.name, 'table');
      i = ref.nextPos;

      while (i < tokens.length && tokens[i].u !== 'AS') i++;
      if (i < tokens.length) i++;
      if (i < tokens.length && tokens[i].u === 'SELECT') {
        tableNode.columns = this.extractSelectListColumns(tokens, i, tokens.length);
        this.analyzeSelect(tokens, i, tokens.length, nodeMap, edges, tableNode.id);
      }
    }
  }

  // Find the index of the main SELECT (outside all CTE bodies) in a CTE token slice
  _findCTEMainSelectIndex(tokens) {
    let i = 0;
    if (i < tokens.length && tokens[i].u === 'WITH') i++;
    if (i < tokens.length && tokens[i].u === 'RECURSIVE') i++;
    // Skip past each CTE name AS (body), …
    while (i < tokens.length) {
      if (!this.isIdentifier(tokens[i])) break;
      i++; // CTE name
      // Optional column list before AS
      if (i < tokens.length && tokens[i].v === '(') {
        const cp = this.findCloseParen(tokens, i);
        if (cp + 1 < tokens.length && tokens[cp + 1].u === 'AS') i = cp + 1;
      }
      if (i < tokens.length && tokens[i].u === 'AS') i++;
      if (i < tokens.length && tokens[i].u === 'NOT') i++;
      if (i < tokens.length && tokens[i].u === 'MATERIALIZED') i++;
      if (i < tokens.length && tokens[i].v === '(') {
        i = this.findCloseParen(tokens, i) + 1;
      }
      if (i < tokens.length && tokens[i].v === ',') { i++; continue; }
      break;
    }
    return (i < tokens.length && tokens[i].u === 'SELECT') ? i : -1;
  }

  // ── UPDATE ─────────────────────────────────────────────────────

  analyzeUpdate(tokens, nodeMap, edges) {
    let i = 1;
    const ref = this.readTableRef(tokens, i);
    if (!ref) return;

    const targetNode = this.getOrCreateNode(nodeMap, ref.name, 'table');
    if (ref.alias) this.addAlias(ref.alias, ref.name);
    i = ref.nextPos;

    // Look for FROM at depth 0
    let depth = 0;
    for (let j = i; j < tokens.length; j++) {
      if (tokens[j].v === '(') depth++;
      if (tokens[j].v === ')') depth--;
      if (depth === 0 && tokens[j].u === 'FROM') {
        // Create a synthetic SELECT-like analysis starting at FROM
        const fromTables = this.analyzeSelectPart(tokens, j, tokens.length, nodeMap, edges, null);
        for (const t of fromTables) {
          edges.push({
            source: t.id,
            target: targetNode.id,
            type: 'data_flow',
            label: 'UPDATE',
            condition: ''
          });
        }
        break;
      }
    }
  }

  // ── DELETE ─────────────────────────────────────────────────────

  analyzeDelete(tokens, nodeMap, edges) {
    let i = 1;
    if (i < tokens.length && tokens[i].u === 'FROM') i++;
    const ref = this.readTableRef(tokens, i);
    if (ref) {
      this.getOrCreateNode(nodeMap, ref.name, 'table');
      if (ref.alias) this.addAlias(ref.alias, ref.name);
    }

    // Check for USING clause (PostgreSQL)
    let depth = 0;
    for (let j = (ref ? ref.nextPos : i); j < tokens.length; j++) {
      if (tokens[j].v === '(') depth++;
      if (tokens[j].v === ')') depth--;
      if (depth === 0 && tokens[j].u === 'USING') {
        j++;
        const uRef = this.readTableRef(tokens, j);
        if (uRef && ref) {
          const uNode = this.getOrCreateNode(nodeMap, uRef.name, 'table');
          edges.push({
            source: uNode.id,
            target: nodeMap.get(ref.name.toLowerCase()).id,
            type: 'join',
            label: 'USING',
            condition: ''
          });
        }
        break;
      }
    }
  }

  // ── MERGE ──────────────────────────────────────────────────────

  analyzeMerge(tokens, nodeMap, edges) {
    let i = 1;
    if (i < tokens.length && tokens[i].u === 'INTO') i++;
    const targetRef = this.readTableRef(tokens, i);
    if (!targetRef) return;

    const targetNode = this.getOrCreateNode(nodeMap, targetRef.name, 'table');
    if (targetRef.alias) this.addAlias(targetRef.alias, targetRef.name);
    i = targetRef.nextPos;

    if (i < tokens.length && tokens[i].u === 'USING') {
      i++;
      const result = this.readTableOrSubquery(tokens, i, tokens.length, nodeMap, edges);
      if (result) {
        edges.push({
          source: result.node.id,
          target: targetNode.id,
          type: 'data_flow',
          label: 'MERGE',
          condition: ''
        });
      }
    }
  }

  // ── CTE (WITH) ────────────────────────────────────────────────

  analyzeCTE(tokens, nodeMap, edges, outerTargetId) {
    let i = 0;
    if (tokens[i].u === 'WITH') i++;
    if (i < tokens.length && tokens[i].u === 'RECURSIVE') i++;

    // Parse CTEs
    while (i < tokens.length) {
      if (!this.isIdentifier(tokens[i])) break;

      const cteName = tokens[i].v;
      i++;

      // Skip column list
      if (i < tokens.length && tokens[i].v === '(') {
        // Check if this is a column list or the AS query
        // Column list: (col1, col2) followed by AS
        // Need to check if after close paren we see AS
        const cp = this.findCloseParen(tokens, i);
        if (cp + 1 < tokens.length && tokens[cp + 1].u === 'AS') {
          i = cp + 1;
        }
      }

      if (i < tokens.length && tokens[i].u === 'AS') i++;

      // Optional MATERIALIZED / NOT MATERIALIZED
      if (i < tokens.length && tokens[i].u === 'NOT') i++;
      if (i < tokens.length && tokens[i].u === 'MATERIALIZED') i++;

      if (i < tokens.length && tokens[i].v === '(') {
        const cp = this.findCloseParen(tokens, i);
        const cteNode = this.getOrCreateNode(nodeMap, cteName, 'cte');
        this.addAlias(cteName, cteName);

        cteNode.columns = this.extractSelectListColumns(tokens, i + 1, cp);
        this.analyzeSelect(tokens, i + 1, cp, nodeMap, edges, cteNode.id);

        i = cp + 1;
      }

      if (i < tokens.length && tokens[i].v === ',') { i++; continue; }
      break;
    }

    // Main query after CTEs
    if (i < tokens.length) {
      const first = tokens[i].u;
      let targetId = outerTargetId || null;

      if (first === 'SELECT') {
        // If no outer target, create a "Query Result" output node
        if (!targetId) {
          const resultNode = this.getOrCreateNode(nodeMap, 'Query Result', 'result');
          targetId = resultNode.id;
          // Extract output column names from the SELECT list
          resultNode.columns = this.extractSelectListColumns(tokens, i, tokens.length);
        }
        this.analyzeSelect(tokens, i, tokens.length, nodeMap, edges, targetId);
      } else if (first === 'INSERT') {
        this.analyzeInsert(tokens.slice(i), nodeMap, edges);
      } else if (first === 'UPDATE') {
        this.analyzeUpdate(tokens.slice(i), nodeMap, edges);
      } else if (first === 'DELETE') {
        this.analyzeDelete(tokens.slice(i), nodeMap, edges);
      }
    }
  }

  // ── Subquery scanning ─────────────────────────────────────────

  scanSubqueries(tokens, start, end, nodeMap, edges) {
    for (let i = start; i < end; i++) {
      if (tokens[i].v === '(' && this.isSubquery(tokens, i)) {
        const cp = this.findCloseParen(tokens, i);
        this.analyzeSelect(tokens, i + 1, cp, nodeMap, edges, null);
        i = cp;
      }
    }
  }

  // ── WHERE condition analysis ──────────────────────────────────

  analyzeWhereConditions(sql, nodeMap, edges) {
    // Find patterns like alias.col = alias.col in WHERE/ON clauses
    const pattern = /\b(\w+)\.(\w+)\s*(?:=|<>|!=|<=|>=|<|>)\s*(\w+)\.(\w+)\b/g;
    let match;

    while ((match = pattern.exec(sql)) !== null) {
      const left = this.resolveAlias(match[1]);
      const right = this.resolveAlias(match[3]);

      if (left !== right && nodeMap.has(left) && nodeMap.has(right)) {
        // Check if a join edge already exists between these two
        const directExists = edges.some(e =>
          (e.type === 'join' || e.type === 'where_join') && (
            (e.source === left && e.target === right) ||
            (e.source === right && e.target === left)
          )
        );

        // Also check hub model: both tables already share a common target
        const leftTargets = new Set(
          edges
            .filter(e => e.source === left && (e.type === 'join' || e.type === 'data_flow'))
            .map(e => e.target)
        );
        const hubExists = edges.some(
          e => e.source === right && (e.type === 'join' || e.type === 'data_flow') && leftTargets.has(e.target)
        );

        const exists = directExists || hubExists;

        if (!exists) {
          edges.push({
            source: left,
            target: right,
            type: 'where_join',
            label: `${match[1]}.${match[2]} = ${match[3]}.${match[4]}`,
            condition: `${match[1]}.${match[2]} = ${match[3]}.${match[4]}`
          });
        }
      }
    }

    // Oracle: detect (+) old-style outer join syntax
    if (this.dialect === 'oracle') {
      const oracleJoinPattern = /\b(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)\s*\(\+\)/g;
      while ((match = oracleJoinPattern.exec(sql)) !== null) {
        const left = this.resolveAlias(match[1]);
        const right = this.resolveAlias(match[3]);
        if (left !== right && nodeMap.has(left) && nodeMap.has(right)) {
          const exists = edges.some(e =>
            (e.source === left && e.target === right) ||
            (e.source === right && e.target === left)
          );
          if (!exists) {
            edges.push({
              source: left,
              target: right,
              type: 'join',
              label: 'LEFT JOIN (+)',
              condition: `${match[1]}.${match[2]} = ${match[3]}.${match[4]}(+)`
            });
          }
        }
      }
      // Reverse direction: a.col(+) = b.col
      const oracleJoinPattern2 = /\b(\w+)\.(\w+)\s*\(\+\)\s*=\s*(\w+)\.(\w+)\b/g;
      while ((match = oracleJoinPattern2.exec(sql)) !== null) {
        const left = this.resolveAlias(match[1]);
        const right = this.resolveAlias(match[3]);
        if (left !== right && nodeMap.has(left) && nodeMap.has(right)) {
          const exists = edges.some(e =>
            (e.source === left && e.target === right) ||
            (e.source === right && e.target === left)
          );
          if (!exists) {
            edges.push({
              source: right,
              target: left,
              type: 'join',
              label: 'LEFT JOIN (+)',
              condition: `${match[1]}.${match[2]}(+) = ${match[3]}.${match[4]}`
            });
          }
        }
      }
    }
  }

  // ── Utilities ─────────────────────────────────────────────────

  isSubquery(tokens, pos) {
    if (pos + 1 < tokens.length && tokens[pos].v === '(') {
      const next = tokens[pos + 1].u;
      return next === 'SELECT' || next === 'WITH';
    }
    return false;
  }

  findCloseParen(tokens, openPos) {
    let depth = 1;
    for (let i = openPos + 1; i < tokens.length; i++) {
      if (tokens[i].v === '(') depth++;
      if (tokens[i].v === ')') { depth--; if (depth === 0) return i; }
    }
    return tokens.length - 1;
  }

  isIdentifier(token) {
    return token && (token.t === 'W' || token.t === 'I');
  }

  isKeyword(word) {
    const kw = new Set([
      'SELECT', 'FROM', 'WHERE', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS',
      'NATURAL', 'ON', 'AND', 'OR', 'NOT', 'IN', 'EXISTS', 'BETWEEN', 'LIKE', 'IS',
      'NULL', 'GROUP', 'BY', 'HAVING', 'ORDER', 'ASC', 'DESC', 'LIMIT', 'OFFSET',
      'UNION', 'EXCEPT', 'INTERSECT', 'ALL', 'DISTINCT', 'INSERT', 'INTO', 'VALUES',
      'UPDATE', 'SET', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'TABLE', 'VIEW', 'INDEX',
      'WITH', 'AS', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'CAST', 'OUTER',
      'FETCH', 'WINDOW', 'PARTITION', 'ROWS', 'RANGE', 'OVER', 'USING', 'LATERAL',
      'RECURSIVE', 'MATERIALIZED', 'REPLACE', 'TRUE', 'FALSE', 'TOP', 'MERGE',
      'MATCHED', 'FOR', 'IF', 'GRANT', 'REVOKE', 'PRIMARY', 'KEY', 'FOREIGN',
      'REFERENCES', 'CHECK', 'CONSTRAINT', 'DEFAULT', 'UNIQUE', 'TRIGGER',
      // Dialect-specific keywords
      'APPLY', 'CONNECT', 'START', 'PRIOR', 'MINUS', 'OPTION',
      'STRAIGHT_JOIN', 'ILIKE', 'OUTPUT', 'PERCENT', 'TIES',
      'NOLOCK', 'ROWLOCK', 'READUNCOMMITTED', 'FORCE', 'USE', 'IGNORE',
    ]);
    return kw.has(word);
  }

  extractColumns(sql, nodeMap) {
    // Collect qualified column refs: alias.column or table.column
    const qualifiedPattern = /\b(\w+)\s*\.\s*(\w+)\b/g;
    let match;
    const columnsByTable = new Map();

    // Aggregation / function names to exclude
    const skipWords = new Set([
      'count', 'sum', 'avg', 'min', 'max', 'coalesce', 'nullif',
      'cast', 'convert', 'trim', 'upper', 'lower', 'length', 'substr',
      'substring', 'replace', 'concat', 'round', 'floor', 'ceil',
      'abs', 'now', 'date', 'year', 'month', 'day', 'extract',
      'row_number', 'rank', 'dense_rank', 'lead', 'lag',
      'first_value', 'last_value', 'ntile', 'string_agg', 'listagg',
      'group_concat',
    ]);

    while ((match = qualifiedPattern.exec(sql)) !== null) {
      const prefix = match[1];
      const col = match[2];

      if (skipWords.has(prefix.toLowerCase())) continue;
      if (skipWords.has(col.toLowerCase())) continue;

      // Skip pure numeric or wildcard
      if (/^\d+$/.test(col) || col === '*') continue;

      const tableId = this.resolveAlias(prefix);
      if (!nodeMap.has(tableId)) continue;

      if (!columnsByTable.has(tableId)) {
        columnsByTable.set(tableId, new Set());
      }
      columnsByTable.get(tableId).add(col);
    }

    // Also scan SELECT list for unqualified columns when there's only one table
    if (nodeMap.size === 1) {
      const singleId = nodeMap.keys().next().value;
      const selectMatch = sql.match(/\bSELECT\s+(.*?)\s+FROM\b/is);
      if (selectMatch) {
        const cols = selectMatch[1].split(',').map(c => c.trim().replace(/\s+AS\s+.*/i, '').trim());
        if (!columnsByTable.has(singleId)) columnsByTable.set(singleId, new Set());
        for (const c of cols) {
          if (c !== '*' && /^\w+$/.test(c)) columnsByTable.get(singleId).add(c);
        }
      }
    }

    // Assign columns to nodes (preserve INSERT column lists if already set)
    for (const [tableId, colSet] of columnsByTable) {
      const node = nodeMap.get(tableId);
      if (!node) continue;
      const existing = new Set(node.columns.map(c => c.toLowerCase()));
      for (const col of colSet) {
        if (!existing.has(col.toLowerCase())) {
          node.columns.push(col);
          existing.add(col.toLowerCase());
        }
      }
    }
  }

  extractGroupByColumns(sql, nodeMap) {
    const cleaned = this.removeComments(sql);
    const tokens = this.tokenize(cleaned);

    let i = 0;
    while (i < tokens.length) {
      // Look for GROUP BY
      if (tokens[i].u === 'GROUP' && i + 1 < tokens.length && tokens[i + 1].u === 'BY') {
        i += 2; // skip GROUP BY
        const endKw = new Set(['HAVING', 'ORDER', 'LIMIT', 'UNION', 'EXCEPT',
          'INTERSECT', 'FETCH', 'OFFSET', 'WINDOW', 'FOR', 'INSERT', 'UPDATE',
          'DELETE', 'SELECT', 'WITH', 'CREATE']);

        while (i < tokens.length && !endKw.has(tokens[i].u) && tokens[i].v !== ';' && tokens[i].v !== ')') {
          // Look for alias.column or bare column
          if (i + 2 < tokens.length && tokens[i + 1].v === '.') {
            const prefix = tokens[i].v;
            const col = tokens[i + 2].v;
            const tableId = this.resolveAlias(prefix);
            const node = nodeMap.get(tableId);
            if (node) {
              if (!node.groupByColumns) node.groupByColumns = new Set();
              node.groupByColumns.add(col.toLowerCase());
            }
            i += 3;
          } else if (tokens[i].t === 'word' && tokens[i].v !== ',') {
            // Bare column — assign to single table or skip
            const col = tokens[i].v;
            if (nodeMap.size === 1) {
              const node = nodeMap.values().next().value;
              if (!node.groupByColumns) node.groupByColumns = new Set();
              node.groupByColumns.add(col.toLowerCase());
            }
            i++;
          } else {
            i++;
          }
        }
      } else {
        i++;
      }
    }

    // Convert Sets to arrays for serialization
    for (const node of nodeMap.values()) {
      if (node.groupByColumns) {
        node.groupByColumns = Array.from(node.groupByColumns);
      } else {
        node.groupByColumns = [];
      }
    }
  }

  extractWhereFilters(sql, nodeMap) {
    // Find all WHERE clauses and extract individual conditions per table
    const cleaned = this.removeComments(sql);
    const tokens = this.tokenize(cleaned);

    let i = 0;
    while (i < tokens.length) {
      if (tokens[i].u === 'WHERE') {
        i++;
        // Collect tokens until end of WHERE clause
        const endKw = new Set(['GROUP', 'ORDER', 'HAVING', 'LIMIT', 'UNION', 'EXCEPT',
          'INTERSECT', 'FETCH', 'OFFSET', 'WINDOW', 'FOR', 'INSERT', 'UPDATE', 'DELETE',
          'CREATE', 'INTO']);
        let depth = 0;
        const whereStart = i;

        while (i < tokens.length) {
          if (tokens[i].v === '(') depth++;
          if (tokens[i].v === ')') {
            if (depth === 0) break;
            depth--;
          }
          if (depth === 0 && endKw.has(tokens[i].u)) break;
          i++;
        }

        // Split WHERE clause into individual conditions (split on AND/OR at depth 0)
        const conditions = this.splitWhereConditions(tokens, whereStart, i);

        for (const cond of conditions) {
          // Find which tables this condition references
          const refs = [...cond.matchAll(/\b(\w+)\s*\./g)]
            .map(m => this.resolveAlias(m[1]))
            .filter(id => nodeMap.has(id));

          // Also check for unqualified column comparisons with literals
          // (these apply to all tables in scope but we skip those)

          const uniqueRefs = [...new Set(refs)];

          // If condition references exactly one table, it's a filter on that table
          // If it references two tables, it's a join condition (already handled)
          if (uniqueRefs.length === 1) {
            const node = nodeMap.get(uniqueRefs[0]);
            if (node && !node.filters.includes(cond.trim())) {
              node.filters.push(cond.trim());
            }
          } else if (uniqueRefs.length === 0) {
            // Unqualified condition — try to find if single table in scope
            // Skip for now (ambiguous)
          }
        }
      }
      i++;
    }
  }

  splitWhereConditions(tokens, start, end) {
    const conditions = [];
    let depth = 0;
    let condStart = start;

    for (let i = start; i < end; i++) {
      if (tokens[i].v === '(') depth++;
      if (tokens[i].v === ')') depth--;
      if (depth === 0 && (tokens[i].u === 'AND' || tokens[i].u === 'OR')) {
        const text = this.tokensToString(tokens, condStart, i);
        if (text) conditions.push(text);
        condStart = i + 1;
      }
    }
    // Last condition
    const text = this.tokensToString(tokens, condStart, end);
    if (text) conditions.push(text);

    return conditions;
  }

  tokensToString(tokens, start, end) {
    let result = '';
    for (let i = start; i < end; i++) {
      const t = tokens[i];
      if (t.v === '.') {
        result += '.';
      } else if (i > start && tokens[i - 1].v === '.') {
        result += t.v;
      } else {
        if (result && !/[(\s]$/.test(result)) result += ' ';
        result += t.v;
      }
    }
    return result.trim();
  }

  deduplicateEdges(edges) {
    const seen = new Set();
    const result = [];
    for (const e of edges) {
      const key = `${e.source}|${e.target}|${e.type}`;
      if (!seen.has(key)) {
        seen.add(key);
        result.push(e);
      }
    }
    return result;
  }
}
