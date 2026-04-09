/**
 * SQL Visualizer - Main Application
 */
(function () {
  'use strict';

  // ── State ────────────────────────────────────────────────────

  let editor;
  let parser;
  let diagram;

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
LEFT JOIN stores s ON o.store_id = s.id;`,

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
GROUP BY d.department_name;`
  };

  // ── Initialization ───────────────────────────────────────────

  function init() {
    parser = new SQLParser();
    diagram = new DiagramRenderer('diagram-container');

    initEditor();
    initDivider();
    bindEvents();

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
      const graph = parser.parse(sql);

      if (graph.nodes.length === 0) {
        diagram.showError('No tables found in the query');
        return;
      }

      diagram.render(graph);
    } catch (err) {
      console.error('Parse error:', err);
      diagram.showError('Error parsing SQL: ' + err.message);
    }
  }

  // ── Bootstrap ────────────────────────────────────────────────

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
