/**
 * SQL Visualizer - Main Application
 */
(function () {
  'use strict';

  // ── State ────────────────────────────────────────────────────

  let editor;
  let parser;
  let diagram;
  let lastGraph = null;
  let editorMarks = [];       // Active CodeMirror text markers
  let positionIndex = null;   // Maps identifiers to editor positions
  let hoverTimeout = null;

  // ── Sample Queries ───────────────────────────────────────────

  const sampleQueries = {
    'simple-join': `SELECT
  e.name,
  e.email,
  d.department_name
FROM employees e
JOIN departments d
  ON e.department_id = d.id;`,

    'multi-join': `SELECT
  o.order_id,
  c.name AS customer_name,
  p.product_name,
  oi.quantity,
  s.store_name
FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN order_items oi ON o.id = oi.order_id
JOIN products p ON oi.product_id = p.id
LEFT JOIN stores s ON o.store_id = s.id
WHERE o.status = 'completed'
  AND c.region = 'US'
  AND p.category = 'Electronics';`,

    'subquery': `SELECT
  e.name,
  e.salary,
  e.department_id
FROM employees e
JOIN (
  SELECT department_id, AVG(salary) AS avg_sal
  FROM employees
  GROUP BY department_id
) dept_avg ON e.department_id = dept_avg.department_id
WHERE e.salary > dept_avg.avg_sal;`,

    'insert-select': `INSERT INTO employee_archive (id, name, department, salary)
SELECT
  e.id,
  e.name,
  d.department_name,
  e.salary
FROM employees e
JOIN departments d ON e.department_id = d.id
WHERE e.status = 'inactive';`,

    'create-view': `CREATE VIEW monthly_sales AS
SELECT
  s.store_name,
  p.category,
  SUM(oi.quantity * oi.unit_price) AS total_sales,
  COUNT(DISTINCT o.order_id) AS order_count
FROM stores s
JOIN orders o ON s.id = o.store_id
JOIN order_items oi ON o.id = oi.order_id
JOIN products p ON oi.product_id = p.id
GROUP BY s.store_name, p.category;`,

    'cte': `WITH regional_sales AS (
  SELECT
    r.region_name,
    SUM(o.amount) AS total_sales
  FROM regions r
  JOIN orders o ON r.id = o.region_id
  GROUP BY r.region_name
),
top_regions AS (
  SELECT region_name
  FROM regional_sales
  WHERE total_sales > (
    SELECT SUM(total_sales) / 10
    FROM regional_sales
  )
)
SELECT
  o.region_id,
  p.product_name,
  SUM(o.quantity) AS product_units,
  SUM(o.amount) AS product_sales
FROM orders o
JOIN products p ON o.product_id = p.id
WHERE o.region_id IN (
  SELECT r.id
  FROM regions r
  JOIN top_regions tr ON r.region_name = tr.region_name
)
GROUP BY o.region_id, p.product_name;`,

    'complex': `-- Multi-statement: View + Insert
CREATE VIEW vsal AS
SELECT
  a.deptno AS "Department",
  a.num_emp / b.total_count AS "Employees",
  a.sal_sum / b.total_sal AS "Salary"
FROM (
  SELECT deptno, COUNT(*) AS num_emp, SUM(sal) AS sal_sum
  FROM scott.emp
  WHERE city = 'NYC'
  GROUP BY deptno
) a,
(
  SELECT COUNT(*) AS total_count, SUM(sal) AS total_sal
  FROM scott.emp
  WHERE city = 'NYC'
) b;

INSERT INTO quarterly_report (dept, emp_count, total_salary)
SELECT
  d.department_name,
  COUNT(e.id),
  SUM(e.salary)
FROM employees e
JOIN departments d ON e.department_id = d.id
LEFT JOIN locations l ON d.location_id = l.id
GROUP BY d.department_name;`,

    'complex-ivo': `WITH recent_orders AS (
  -- orders in the last 12 months
  SELECT o.*
  FROM orders o
  WHERE o.created_at >= (current_date - INTERVAL '12 months')
),
user_order_stats AS (
  -- per-user aggregates: total spent, avg order value, last order date, order count
  SELECT
    u.id AS user_id,
    u.name,
    u.country,
    COUNT(ro.id)                         AS orders_count,
    SUM(ro.total_amount)                 AS total_spent,
    AVG(NULLIF(ro.total_amount,0))       AS avg_order_value,
    MAX(ro.created_at)                   AS last_order_at
  FROM users u
  LEFT JOIN recent_orders ro ON ro.user_id = u.id
  GROUP BY u.id, u.name, u.country
),
user_top_category AS (
  -- most-purchased product category per user (by quantity)
  SELECT ut.user_id, ut.category
  FROM (
    SELECT
      oi_order.user_id,
      p.category,
      SUM(oi_order.quantity) AS qty,
      ROW_NUMBER() OVER (PARTITION BY oi_order.user_id ORDER BY SUM(oi_order.quantity) DESC, p.category) AS rn
    FROM (
      SELECT o.id AS order_id, o.user_id
      FROM recent_orders o
    ) AS oi_order
    JOIN order_items oi ON oi.order_id = oi_order.order_id
    JOIN products p ON p.id = oi.product_id
    GROUP BY oi_order.user_id, p.category
  ) ut
  WHERE ut.rn = 1
),
user_review_score AS (
  -- average rating given by user in last 12 months
  SELECT r.user_id, AVG(r.rating) AS avg_rating, COUNT(*) AS reviews_count
  FROM reviews r
  WHERE r.created_at >= (current_date - INTERVAL '12 months')
  GROUP BY r.user_id
)
SELECT
  uos.user_id,
  uos.name,
  uos.country,
  COALESCE(uos.orders_count, 0)         AS orders_count,
  COALESCE(uos.total_spent, 0)::numeric(12,2) AS total_spent,
  COALESCE(uos.avg_order_value, 0)::numeric(10,2) AS avg_order_value,
  utc.category                          AS top_category,
  COALESCE(urs.avg_rating, 0)::numeric(3,2)     AS avg_review_rating,
  uos.last_order_at,
  -- churn flag if no order in last 90 days
  CASE WHEN uos.last_order_at < (current_date - INTERVAL '90 days') OR uos.last_order_at IS NULL THEN TRUE ELSE FALSE END AS is_churned,
  -- rank users by total_spent (desc)
  RANK() OVER (ORDER BY COALESCE(uos.total_spent,0) DESC) AS spend_rank
FROM user_order_stats uos
LEFT JOIN user_top_category utc ON utc.user_id = uos.user_id
LEFT JOIN user_review_score urs ON urs.user_id = uos.user_id
WHERE COALESCE(uos.orders_count,0) > 0
  AND (uos.country = 'US' OR uos.country = 'CA')
ORDER BY total_spent DESC
LIMIT 50 OFFSET 0;`
  };

  // ── Initialization ───────────────────────────────────────────

  function init() {
    parser = new SQLParser();
    diagram = new DiagramRenderer('diagram-container');

    initEditor();
    initDivider();
    bindEvents();
    setupEditorHover();

    // Load default sample
    editor.setValue(sampleQueries['multi-join']);
  }

  function initEditor() {
    editor = CodeMirror.fromTextArea(document.getElementById('sql-editor'), {
      mode: 'text/x-sql',
      theme: 'dracula',
      lineNumbers: true,
      lineWrapping: true,
      matchBrackets: true,
      autoCloseBrackets: true,
      tabSize: 2,
      indentWithTabs: false,
      extraKeys: {
        'Ctrl-Enter': () => visualize(),
        'Cmd-Enter': () => visualize(),
      },
      placeholder: 'Enter your SQL query here...',
    });
  }

  function initDivider() {
    const divider = document.getElementById('divider');
    const leftPanel = document.getElementById('left-panel');
    let startX, startWidth;

    divider.addEventListener('mousedown', (e) => {
      e.preventDefault();
      startX = e.clientX;
      startWidth = leftPanel.offsetWidth;
      divider.classList.add('active');

      const onMove = (e) => {
        const diff = e.clientX - startX;
        const newWidth = Math.max(200, Math.min(window.innerWidth - 300, startWidth + diff));
        leftPanel.style.width = newWidth + 'px';
      };

      const onUp = () => {
        divider.classList.remove('active');
        document.removeEventListener('mousemove', onMove);
        document.removeEventListener('mouseup', onUp);
        // Refresh editor layout
        editor.refresh();
      };

      document.addEventListener('mousemove', onMove);
      document.addEventListener('mouseup', onUp);
    });
  }

  function bindEvents() {
    document.getElementById('btn-visualize').addEventListener('click', visualize);
    document.getElementById('btn-clear').addEventListener('click', () => {
      editor.setValue('');
      editor.focus();
    });
    document.getElementById('btn-zoom-in').addEventListener('click', () => diagram.zoomIn());
    document.getElementById('btn-zoom-out').addEventListener('click', () => diagram.zoomOut());
    document.getElementById('btn-zoom-reset').addEventListener('click', () => diagram.zoomReset());

    document.getElementById('sample-queries').addEventListener('change', (e) => {
      const key = e.target.value;
      if (key && sampleQueries[key]) {
        editor.setValue(sampleQueries[key]);
        e.target.value = '';
        visualize();
      }
    });

    document.getElementById('sql-dialect').addEventListener('change', () => {
      // Update CodeMirror SQL mode for syntax highlighting
      const modeMap = {
        'postgresql': 'text/x-pgsql',
        'oracle': 'text/x-plsql',
        'mysql': 'text/x-mysql',
        'mssql': 'text/x-mssql',
      };
      const dialect = document.getElementById('sql-dialect').value;
      editor.setOption('mode', modeMap[dialect] || 'text/x-sql');
      // Re-visualize if there's content
      if (editor.getValue().trim()) visualize();
    });

    // Window resize
    window.addEventListener('resize', () => {
      if (diagram.svg) diagram.fitToView();
      editor.refresh();
    });
  }

  // ── Visualize ────────────────────────────────────────────────

  function visualize() {
    const sql = editor.getValue().trim();

    if (!sql) {
      diagram.showError('Please enter a SQL query');
      return;
    }

    try {
      const dialect = document.getElementById('sql-dialect').value;
      const graph = parser.parse(sql, dialect);

      if (graph.nodes.length === 0) {
        diagram.showError('No tables found in the query');
        return;
      }

      lastGraph = graph;
      diagram.render(graph);

      // Build position index for cross-highlighting
      positionIndex = buildPositionIndex(editor.getValue(), graph);

      // Wire diagram → editor hover callbacks
      diagram.onNodeHover = (nodeId) => {
        clearEditorHighlights();
        const ranges = positionIndex.nodeRanges[nodeId];
        if (ranges) {
          for (const r of ranges) {
            editorMarks.push(editor.markText(r.from, r.to, { className: 'cm-highlight-table' }));
          }
        }
      };

      diagram.onColumnHover = (nodeId, colName) => {
        clearEditorHighlights();
        // Highlight the column references
        const key = nodeId + '.' + colName.toLowerCase();
        const ranges = positionIndex.colRanges[key];
        if (ranges) {
          for (const r of ranges) {
            editorMarks.push(editor.markText(r.from, r.to, { className: 'cm-highlight-column' }));
          }
        }
        // Also highlight the table
        const tableRanges = positionIndex.nodeRanges[nodeId];
        if (tableRanges) {
          for (const r of tableRanges) {
            editorMarks.push(editor.markText(r.from, r.to, { className: 'cm-highlight-table-dim' }));
          }
        }
      };

      diagram.onHoverOut = () => {
        clearEditorHighlights();
        diagram.clearHighlight();
      };

    } catch (err) {
      console.error('Parse error:', err);
      diagram.showError('Error parsing SQL: ' + err.message);
    }
  }

  // ── Position Index ───────────────────────────────────────────

  function buildPositionIndex(sql, graph) {
    const index = {
      nodeRanges: {},   // nodeId -> [{from, to}]
      colRanges: {},    // "nodeId.col" -> [{from, to}]
      wordMap: {},      // word (lower) -> nodeId (for editor → diagram)
    };

    // Get alias map from parser
    const aliasMap = parser.aliasMap;

    // Build a reverse map: tableId -> [alias, tableName, ...]
    const namesByNode = {};
    for (const node of graph.nodes) {
      const id = node.id;
      namesByNode[id] = new Set([node.name.toLowerCase()]);
      index.wordMap[node.name.toLowerCase()] = id;
    }
    for (const [alias, tableId] of aliasMap) {
      if (namesByNode[tableId]) {
        namesByNode[tableId].add(alias.toLowerCase());
        index.wordMap[alias.toLowerCase()] = tableId;
      }
    }

    // Also map columns to their parent node
    for (const node of graph.nodes) {
      for (const col of node.columns) {
        const key = node.id + '.' + col.toLowerCase();
        index.wordMap[col.toLowerCase()] = { nodeId: node.id, col: col.toLowerCase() };
      }
    }

    // Scan the SQL text for word positions
    const lines = sql.split('\n');
    const wordPattern = /[a-zA-Z_]\w*/g;

    for (let lineNo = 0; lineNo < lines.length; lineNo++) {
      const line = lines[lineNo];
      let m;
      wordPattern.lastIndex = 0;

      while ((m = wordPattern.exec(line)) !== null) {
        const word = m[0].toLowerCase();
        const ch = m.index;
        const from = { line: lineNo, ch: ch };
        const to = { line: lineNo, ch: ch + m[0].length };

        // Check if this is a table name or alias
        for (const [nodeId, names] of Object.entries(namesByNode)) {
          if (names.has(word)) {
            if (!index.nodeRanges[nodeId]) index.nodeRanges[nodeId] = [];
            index.nodeRanges[nodeId].push({ from, to });
          }
        }

        // Check if preceded by "alias." — then it's a column reference
        // Look for pattern: alias.column
        const prefixMatch = line.slice(0, ch).match(/(\w+)\s*\.\s*$/);
        if (prefixMatch) {
          const prefix = prefixMatch[1].toLowerCase();
          const resolvedTable = aliasMap.get(prefix) || prefix;
          if (namesByNode[resolvedTable]) {
            const key = resolvedTable + '.' + word;
            if (!index.colRanges[key]) index.colRanges[key] = [];
            // Mark the full "alias.column" range
            const dotStart = ch - prefixMatch[0].length;
            index.colRanges[key].push({
              from: { line: lineNo, ch: dotStart },
              to: to
            });
          }
        }
      }
    }

    return index;
  }

  // ── Editor → Diagram highlighting ───────────────────────────

  function setupEditorHover() {
    const cmWrapper = editor.getWrapperElement();

    cmWrapper.addEventListener('mousemove', (e) => {
      if (hoverTimeout) clearTimeout(hoverTimeout);
      hoverTimeout = setTimeout(() => {
        handleEditorHover(e);
      }, 50);
    });

    cmWrapper.addEventListener('mouseleave', () => {
      if (hoverTimeout) clearTimeout(hoverTimeout);
      clearEditorHighlights();
      diagram.clearHighlight();
    });
  }

  function handleEditorHover(e) {
    if (!positionIndex || !lastGraph) return;

    const pos = editor.coordsChar({ left: e.clientX, top: e.clientY });
    if (!pos || pos.outside) {
      diagram.clearHighlight();
      return;
    }

    // Get the word at cursor position
    const line = editor.getLine(pos.line);
    if (!line) { diagram.clearHighlight(); return; }

    const word = getWordAt(line, pos.ch);
    if (!word) { diagram.clearHighlight(); return; }

    const wordLower = word.toLowerCase();

    // Check if it's a table/alias name
    const aliasMap = parser.aliasMap;
    const resolvedTable = aliasMap.get(wordLower) || wordLower;

    // Find if this word is a known table/alias
    if (positionIndex.nodeRanges[resolvedTable]) {
      diagram.clearHighlight();
      diagram.highlightNode(resolvedTable);
      return;
    }

    // Check if it's part of alias.column pattern
    const beforeDot = line.slice(0, pos.ch).match(/(\w+)\s*\.\s*$/);
    const afterDot = line.slice(pos.ch).match(/^\.?\s*(\w+)/);

    if (beforeDot) {
      // Cursor is on "column" in "alias.column"
      const prefix = beforeDot[1].toLowerCase();
      const tableId = aliasMap.get(prefix) || prefix;
      if (positionIndex.nodeRanges[tableId]) {
        diagram.clearHighlight();
        diagram.highlightColumn(tableId, wordLower);
        return;
      }
    }

    if (afterDot && /\.\s*$/.test(line.slice(0, getWordStart(line, pos.ch)))) {
      // Already handled above
    }

    // Check if word is a column name on any table
    for (const node of lastGraph.nodes) {
      for (const col of node.columns) {
        if (col.toLowerCase() === wordLower) {
          diagram.clearHighlight();
          diagram.highlightColumn(node.id, wordLower);
          return;
        }
      }
    }

    diagram.clearHighlight();
  }

  function getWordAt(line, ch) {
    const start = getWordStart(line, ch);
    let end = ch;
    while (end < line.length && /\w/.test(line[end])) end++;
    if (start === end) return null;
    return line.slice(start, end);
  }

  function getWordStart(line, ch) {
    let start = ch;
    while (start > 0 && /\w/.test(line[start - 1])) start--;
    return start;
  }

  function clearEditorHighlights() {
    for (const mark of editorMarks) {
      mark.clear();
    }
    editorMarks = [];
  }

  // ── Bootstrap ────────────────────────────────────────────────

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
