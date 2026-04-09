/**
 * Diagram Renderer - D3.js + dagre based SQL visualization.
 */
class DiagramRenderer {
  constructor(containerId) {
    this.container = document.getElementById(containerId);
    this.svg = null;
    this.g = null;
    this.zoom = null;
    this.tooltip = document.getElementById('tooltip');

    // Color map for node types
    this.colors = {
      table: '#4a9eff',
      view: '#b370f0',
      cte: '#ff9f43',
      subquery: '#6c757d',
    };

    // Sizing
    this.charWidth = 7.5;
    this.lineHeight = 24;
    this.headerHeight = 40;
    this.padding = 14;
    this.minNodeWidth = 150;
    this.colIconWidth = 18;

    // Live node positions: nodeId -> { x, y, width, height } (x,y = top-left)
    this.nodePositions = new Map();
    // Edge data: array of { source, target, type, label, condition }
    this.edgeData = [];
    // Hover callbacks
    this.onNodeHover = null;   // (nodeId) => void
    this.onColumnHover = null; // (nodeId, colName) => void
    this.onHoverOut = null;    // () => void
  }

  render(graph) {
    this.clear();
    if (!graph.nodes.length) return;

    // Remove placeholder
    const placeholder = document.getElementById('diagram-placeholder');
    if (placeholder) placeholder.style.display = 'none';

    // Create SVG
    this.svg = d3.select(this.container)
      .append('svg')
      .attr('width', '100%')
      .attr('height', '100%');

    // Add defs for arrow markers
    const defs = this.svg.append('defs');
    this.createArrowMarkers(defs);

    // Main group for zoom/pan
    this.g = this.svg.append('g').attr('class', 'canvas');

    // Setup zoom
    this.zoom = d3.zoom()
      .scaleExtent([0.1, 4])
      .on('zoom', (event) => {
        this.g.attr('transform', event.transform);
      });
    this.svg.call(this.zoom);

    // Compute node dimensions
    const nodesSized = this.computeNodeSizes(graph.nodes);

    // Layout with dagre
    const layout = this.computeLayout(nodesSized, graph.edges);

    // Store initial node positions from dagre layout
    this.nodePositions.clear();
    layout.nodes().forEach(nodeId => {
      const node = layout.node(nodeId);
      if (node) {
        this.nodePositions.set(nodeId, {
          x: node.x - node.width / 2,
          y: node.y - node.height / 2,
          width: node.width,
          height: node.height,
        });
      }
    });

    // Store edge data for re-rendering
    this.edgeData = [];
    layout.edges().forEach(edgeId => {
      const edge = layout.edge(edgeId);
      if (edge) {
        this.edgeData.push({
          source: edgeId.v,
          target: edgeId.w,
          type: edge.type || 'join',
          label: edge.label || '',
          condition: edge.condition || '',
        });
      }
    });

    // Render edges first (below nodes)
    this.edgesG = this.g.append('g').attr('class', 'edges');
    this.renderEdges();

    // Render nodes
    this.renderNodes(layout);

    // Fit to view
    this.fitToView();

    // Add legend
    this.addLegend(graph);
  }

  clear() {
    if (this.svg) {
      this.svg.remove();
      this.svg = null;
    }
    this.edgesG = null;
    this.nodePositions.clear();
    this.edgeData = [];
    // Remove legend
    const legend = document.getElementById('legend');
    if (legend) legend.remove();
    // Remove errors
    const err = this.container.querySelector('.error-banner');
    if (err) err.remove();
  }

  createArrowMarkers(defs) {
    const types = [
      { id: 'arrow-join', color: '#7a8aaa' },
      { id: 'arrow-flow', color: '#4a9eff' },
      { id: 'arrow-where', color: '#f0c040' },
      { id: 'arrow-implicit', color: '#7a8aaa' },
    ];

    for (const t of types) {
      defs.append('marker')
        .attr('id', t.id)
        .attr('viewBox', '0 0 10 10')
        .attr('refX', 10)
        .attr('refY', 5)
        .attr('markerWidth', 8)
        .attr('markerHeight', 8)
        .attr('orient', 'auto-start-reverse')
        .append('path')
        .attr('d', 'M 0 0 L 10 5 L 0 10 z')
        .attr('fill', t.color);
    }
  }

  computeNodeSizes(nodes) {
    return nodes.map(node => {
      const titleLen = node.name.length + (node.type !== 'table' ? node.type.length + 3 : 0);
      const maxColLen = node.columns.reduce((max, c) => Math.max(max, c.length), 0);
      const headerW = titleLen * (this.charWidth + 0.5) + this.padding * 2 + 8;
      const colW = maxColLen * this.charWidth + this.padding * 2 + this.colIconWidth + 4;
      const width = Math.max(this.minNodeWidth, headerW, colW);

      const colCount = Math.min(node.columns.length, 12);
      const hasMore = node.columns.length > 12;
      const bodyHeight = colCount > 0
        ? colCount * this.lineHeight + (hasMore ? this.lineHeight : 0) + 10
        : 10;

      // WHERE section: collapsed = one row for the badge
      const hasFilters = (node.filters || []).length > 0;
      const whereHeight = hasFilters ? this.lineHeight + 10 : 0;

      const height = this.headerHeight + bodyHeight + whereHeight;

      return { ...node, width, height, _whereExpanded: false };
    });
  }

  computeLayout(nodes, edges) {
    const g = new dagre.graphlib.Graph();
    g.setGraph({
      rankdir: 'LR',
      nodesep: 40,
      ranksep: 80,
      marginx: 30,
      marginy: 30,
      edgesep: 20,
    });
    g.setDefaultEdgeLabel(() => ({}));

    for (const node of nodes) {
      g.setNode(node.id, { width: node.width, height: node.height, ...node });
    }

    for (const edge of edges) {
      if (g.hasNode(edge.source) && g.hasNode(edge.target)) {
        g.setEdge(edge.source, edge.target, {
          type: edge.type,
          label: edge.label || '',
          condition: edge.condition || '',
        });
      }
    }

    dagre.layout(g);
    return g;
  }

  renderNodes(layout) {
    const self = this;
    const nodesG = this.g.append('g').attr('class', 'nodes');

    layout.nodes().forEach(nodeId => {
      const node = layout.node(nodeId);
      if (!node) return;

      const x = node.x - node.width / 2;
      const y = node.y - node.height / 2;
      const color = this.colors[node.type] || this.colors.table;
      const cols = (node.columns || []).slice(0, 12);
      const hasMore = (node.columns || []).length > 12;

      const group = nodesG.append('g')
        .attr('class', 'node-group')
        .attr('data-node-id', nodeId)
        .attr('transform', `translate(${x}, ${y})`)
        .call(this.createDrag(nodeId));

      // Outer border
      group.append('rect')
        .attr('class', 'node-body')
        .attr('width', node.width)
        .attr('height', node.height);

      // Header background
      group.append('rect')
        .attr('class', 'node-header-bg')
        .attr('width', node.width)
        .attr('height', this.headerHeight)
        .attr('fill', color);

      // Invisible header hover zone for table-level hover
      group.append('rect')
        .attr('class', 'node-header-hover')
        .attr('width', node.width)
        .attr('height', this.headerHeight)
        .attr('fill', 'transparent')
        .style('cursor', 'pointer')
        .on('mouseenter', () => {
          if (self.onNodeHover) self.onNodeHover(nodeId);
        })
        .on('mouseleave', () => {
          if (self.onHoverOut) self.onHoverOut();
        });

      // Icon
      const icon = node.type === 'view' ? '◫' : node.type === 'cte' ? '⊞' : node.type === 'subquery' ? '◻' : '⊞';
      group.append('text')
        .attr('class', 'node-icon')
        .attr('x', this.padding - 1)
        .attr('y', this.headerHeight / 2 + 1)
        .attr('dominant-baseline', 'central')
        .text(icon);

      // Table name
      group.append('text')
        .attr('class', 'node-title')
        .attr('x', this.padding + 18)
        .attr('y', this.headerHeight / 2 + 1)
        .attr('dominant-baseline', 'central')
        .text(node.name);

      // Type badge
      if (node.type !== 'table') {
        group.append('text')
          .attr('class', 'node-type-badge')
          .attr('x', node.width - this.padding)
          .attr('y', this.headerHeight / 2 + 1)
          .attr('dominant-baseline', 'central')
          .attr('text-anchor', 'end')
          .text(node.type.toUpperCase());
      }

      // Header separator line
      group.append('line')
        .attr('class', 'node-separator')
        .attr('x1', 0)
        .attr('y1', this.headerHeight)
        .attr('x2', node.width)
        .attr('y2', this.headerHeight);

      // Columns
      if (cols.length > 0) {
        cols.forEach((col, idx) => {
          const cy = this.headerHeight + 6 + (idx + 0.5) * this.lineHeight;
          // Column dot icon
          group.append('circle')
            .attr('class', 'node-col-dot')
            .attr('cx', this.padding + 4)
            .attr('cy', cy)
            .attr('r', 3);

          // Column name
          const isGrouped = (node.groupByColumns || []).includes(col.toLowerCase());
          group.append('text')
            .attr('class', 'node-column-text' + (isGrouped ? ' node-column-grouped' : ''))
            .attr('data-column', col)
            .attr('data-node-id', nodeId)
            .attr('x', this.padding + this.colIconWidth)
            .attr('y', cy)
            .attr('dominant-baseline', 'central')
            .text(col)
            .style('cursor', 'pointer')
            .on('mouseenter', () => {
              if (self.onColumnHover) self.onColumnHover(nodeId, col);
            })
            .on('mouseleave', () => {
              if (self.onHoverOut) self.onHoverOut();
            });
        });

        if (hasMore) {
          const moreY = this.headerHeight + 6 + (cols.length + 0.5) * this.lineHeight;
          group.append('text')
            .attr('class', 'node-column-more')
            .attr('x', this.padding + this.colIconWidth)
            .attr('y', moreY)
            .attr('dominant-baseline', 'central')
            .text(`+ ${node.columns.length - 12} more`);
        }
      } else {
        // No columns found — show placeholder
        group.append('text')
          .attr('class', 'node-column-empty')
          .attr('x', node.width / 2)
          .attr('y', this.headerHeight + (node.height - this.headerHeight) / 2)
          .attr('dominant-baseline', 'central')
          .attr('text-anchor', 'middle')
          .text('(no columns)');
      }

      // WHERE filter section
      const filters = node.filters || [];
      if (filters.length > 0) {
        const colsShown = Math.min((node.columns || []).length, 12);
        const hasMoreCols = (node.columns || []).length > 12;
        const colsSectionH = colsShown > 0
          ? colsShown * this.lineHeight + (hasMoreCols ? this.lineHeight : 0) + 6
          : 0;
        const whereY = this.headerHeight + colsSectionH + 4;

        // Separator above WHERE
        group.append('line')
          .attr('class', 'node-separator')
          .attr('x1', 0)
          .attr('y1', whereY)
          .attr('x2', node.width)
          .attr('y2', whereY);

        // WHERE badge (clickable)
        const whereG = group.append('g')
          .attr('class', 'node-where-group')
          .style('cursor', 'pointer');

        const badgeY = whereY + this.lineHeight / 2 + 4;

        // Filter icon
        whereG.append('text')
          .attr('class', 'node-where-icon')
          .attr('x', this.padding)
          .attr('y', badgeY)
          .attr('dominant-baseline', 'central')
          .text('▸');

        whereG.append('text')
          .attr('class', 'node-where-label')
          .attr('x', this.padding + 14)
          .attr('y', badgeY)
          .attr('dominant-baseline', 'central')
          .text('WHERE');

        // Filter count
        whereG.append('text')
          .attr('class', 'node-where-count')
          .attr('x', this.padding + 62)
          .attr('y', badgeY)
          .attr('dominant-baseline', 'central')
          .text(`(${filters.length})`);

        // Expanded content (hidden initially)
        const expandedG = group.append('g')
          .attr('class', 'node-where-expanded')
          .style('display', 'none');

        filters.forEach((f, idx) => {
          const fy = whereY + this.lineHeight + 6 + idx * this.lineHeight;
          expandedG.append('text')
            .attr('class', 'node-where-text')
            .attr('x', this.padding + 14)
            .attr('y', fy)
            .attr('dominant-baseline', 'central')
            .text(f.length > 40 ? f.slice(0, 38) + '…' : f)
            .append('title').text(f);
        });

        // Click to toggle expand/collapse
        whereG.on('click', (event) => {
          event.stopPropagation();
          const isVisible = expandedG.style('display') !== 'none';
          const arrow = whereG.select('.node-where-icon');

          if (isVisible) {
            // Collapse
            expandedG.style('display', 'none');
            arrow.text('▸');
            // Shrink body rect
            const collapsedH = this.headerHeight
              + (colsShown > 0 ? colsShown * this.lineHeight + (hasMoreCols ? this.lineHeight : 0) + 10 : 10)
              + this.lineHeight + 10;
            group.select('.node-body').attr('height', collapsedH);
            // Update stored position height for edge routing
            const pos = self.nodePositions.get(nodeId);
            if (pos) pos.height = collapsedH;
          } else {
            // Expand
            expandedG.style('display', null);
            arrow.text('▾');
            const expandedH = this.headerHeight
              + (colsShown > 0 ? colsShown * this.lineHeight + (hasMoreCols ? this.lineHeight : 0) + 10 : 10)
              + this.lineHeight + 10
              + filters.length * this.lineHeight + 4;
            group.select('.node-body').attr('height', expandedH);
            const pos = self.nodePositions.get(nodeId);
            if (pos) pos.height = expandedH;
          }
          // Re-render edges for updated box size
          self.renderEdges();
        });
      }
    });
  }

  renderEdges() {
    const self = this;
    // Clear existing edges
    this.edgesG.selectAll('*').remove();

    for (const edgeInfo of this.edgeData) {
      const srcPos = this.nodePositions.get(edgeInfo.source);
      const tgtPos = this.nodePositions.get(edgeInfo.target);
      if (!srcPos || !tgtPos) continue;

      const type = edgeInfo.type;
      const markerId = type === 'data_flow' ? 'arrow-flow' :
        type === 'where_join' ? 'arrow-where' :
          type === 'implicit_join' ? 'arrow-implicit' :
            'arrow-join';

      // Compute connection points on rectangle edges
      const points = this.computeEdgePath(srcPos, tgtPos);

      const lineGen = d3.line()
        .x(d => d.x)
        .y(d => d.y)
        .curve(d3.curveBasis);

      const edgeGroup = this.edgesG.append('g')
        .attr('class', 'edge-group')
        .attr('data-source', edgeInfo.source)
        .attr('data-target', edgeInfo.target);

      edgeGroup.append('path')
        .attr('class', `edge-line ${type}`)
        .attr('d', lineGen(points))
        .attr('marker-end', `url(#${markerId})`);

      // Label
      if (edgeInfo.label) {
        const mid = points[Math.floor(points.length / 2)];
        const labelText = edgeInfo.label;
        const labelWidth = labelText.length * 7 + 12;

        edgeGroup.append('rect')
          .attr('class', 'edge-label-bg')
          .attr('x', mid.x - labelWidth / 2)
          .attr('y', mid.y - 9)
          .attr('width', labelWidth)
          .attr('height', 18)
          .attr('rx', 3);

        edgeGroup.append('text')
          .attr('class', 'edge-label')
          .attr('x', mid.x)
          .attr('y', mid.y + 4)
          .text(labelText);
      }

      // Tooltip on hover
      if (edgeInfo.condition) {
        edgeGroup
          .on('mouseenter', (event) => {
            self.showTooltip(event, edgeInfo.label, edgeInfo.condition);
          })
          .on('mouseleave', () => {
            self.hideTooltip();
          });
      }
    }
  }

  /**
   * Compute a clean edge path between two rectangular nodes.
   * Attaches to the closest sides (left/right/top/bottom centers).
   */
  computeEdgePath(src, tgt) {
    const srcCx = src.x + src.width / 2;
    const srcCy = src.y + src.height / 2;
    const tgtCx = tgt.x + tgt.width / 2;
    const tgtCy = tgt.y + tgt.height / 2;

    const dx = tgtCx - srcCx;
    const dy = tgtCy - srcCy;

    let srcPt, tgtPt;

    // Determine which sides to connect based on relative position
    if (Math.abs(dx) > Math.abs(dy)) {
      // Horizontal dominant
      if (dx > 0) {
        // Target is to the right
        srcPt = { x: src.x + src.width, y: srcCy };
        tgtPt = { x: tgt.x, y: tgtCy };
      } else {
        // Target is to the left
        srcPt = { x: src.x, y: srcCy };
        tgtPt = { x: tgt.x + tgt.width, y: tgtCy };
      }
    } else {
      // Vertical dominant
      if (dy > 0) {
        // Target is below
        srcPt = { x: srcCx, y: src.y + src.height };
        tgtPt = { x: tgtCx, y: tgt.y };
      } else {
        // Target is above
        srcPt = { x: srcCx, y: src.y };
        tgtPt = { x: tgtCx, y: tgt.y + tgt.height };
      }
    }

    // Create intermediate points for a smooth curve
    const midX = (srcPt.x + tgtPt.x) / 2;
    const midY = (srcPt.y + tgtPt.y) / 2;

    if (Math.abs(dx) > Math.abs(dy)) {
      return [
        srcPt,
        { x: midX, y: srcPt.y },
        { x: midX, y: tgtPt.y },
        tgtPt,
      ];
    } else {
      return [
        srcPt,
        { x: srcPt.x, y: midY },
        { x: tgtPt.x, y: midY },
        tgtPt,
      ];
    }
  }

  createDrag(nodeId) {
    const self = this;
    return d3.drag()
      .on('start', function (event) {
        d3.select(this).raise();
      })
      .on('drag', function (event) {
        d3.select(this).attr('transform', `translate(${event.x}, ${event.y})`);

        // Update stored position
        const pos = self.nodePositions.get(nodeId);
        if (pos) {
          pos.x = event.x;
          pos.y = event.y;
        }

        // Re-render all edges
        self.renderEdges();
      });
  }

  fitToView() {
    if (!this.svg || !this.g) return;

    const svgNode = this.svg.node();
    const bounds = this.g.node().getBBox();
    if (bounds.width === 0 || bounds.height === 0) return;

    const fullWidth = svgNode.clientWidth || this.container.clientWidth;
    const fullHeight = svgNode.clientHeight || this.container.clientHeight;

    const padding = 40;
    const scale = Math.min(
      (fullWidth - padding * 2) / bounds.width,
      (fullHeight - padding * 2) / bounds.height,
      1.5 // Don't zoom in too much
    );

    const translateX = (fullWidth - bounds.width * scale) / 2 - bounds.x * scale;
    const translateY = (fullHeight - bounds.height * scale) / 2 - bounds.y * scale;

    this.svg.call(
      this.zoom.transform,
      d3.zoomIdentity.translate(translateX, translateY).scale(scale)
    );
  }

  zoomIn() {
    if (this.svg && this.zoom) {
      this.svg.transition().duration(300).call(this.zoom.scaleBy, 1.3);
    }
  }

  zoomOut() {
    if (this.svg && this.zoom) {
      this.svg.transition().duration(300).call(this.zoom.scaleBy, 0.7);
    }
  }

  zoomReset() {
    this.fitToView();
  }

  // ── Highlighting API ───────────────────────────────────────────

  highlightNode(nodeId) {
    if (!this.g) return;
    // Dim all nodes
    this.g.selectAll('.node-group').classed('dimmed', true);
    // Bright the target node
    this.g.selectAll(`.node-group[data-node-id="${nodeId}"]`)
      .classed('dimmed', false)
      .classed('highlighted', true);
    // Dim all edges, then highlight connected ones
    this.edgesG.selectAll('.edge-group').each(function() {
      const el = d3.select(this);
      const src = el.attr('data-source');
      const tgt = el.attr('data-target');
      if (src === nodeId || tgt === nodeId) {
        el.classed('dimmed', false).selectAll('.edge-line').classed('highlighted', true);
      } else {
        el.classed('dimmed', true);
      }
    });
  }

  highlightColumn(nodeId, colName) {
    if (!this.g) return;
    // Dim all nodes
    this.g.selectAll('.node-group').classed('dimmed', true);
    // Bright the target node
    const nodeGroup = this.g.selectAll(`.node-group[data-node-id="${nodeId}"]`);
    nodeGroup.classed('dimmed', false).classed('highlighted', true);
    // Highlight the specific column
    nodeGroup.selectAll(`.node-column-text[data-column="${colName}"]`)
      .classed('col-highlighted', true);
    // Dim all edges
    this.edgesG.selectAll('.edge-group').classed('dimmed', true);
  }

  clearHighlight() {
    if (!this.g) return;
    this.g.selectAll('.node-group')
      .classed('dimmed', false)
      .classed('highlighted', false);
    this.g.selectAll('.node-column-text')
      .classed('col-highlighted', false);
    this.edgesG.selectAll('.edge-group').classed('dimmed', false);
    this.edgesG.selectAll('.edge-line').classed('highlighted', false);
  }

  showTooltip(event, label, condition) {
    const tt = this.tooltip;
    tt.innerHTML = `<div class="tt-label">${this.escapeHtml(label)}</div><div class="tt-cond">${this.escapeHtml(condition)}</div>`;
    tt.style.left = (event.pageX + 12) + 'px';
    tt.style.top = (event.pageY - 10) + 'px';
    tt.classList.add('visible');
  }

  hideTooltip() {
    this.tooltip.classList.remove('visible');
  }

  escapeHtml(str) {
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
  }

  showError(message) {
    const existing = this.container.querySelector('.error-banner');
    if (existing) existing.remove();

    const banner = document.createElement('div');
    banner.className = 'error-banner';
    banner.textContent = message;
    this.container.appendChild(banner);

    setTimeout(() => banner.remove(), 4000);
  }

  addLegend(graph) {
    const existing = document.getElementById('legend');
    if (existing) existing.remove();

    const types = new Set(graph.nodes.map(n => n.type));
    const edgeTypes = new Set(graph.edges.map(e => e.type));

    const legend = document.createElement('div');
    legend.id = 'legend';

    const items = [];

    if (types.has('table')) items.push({ color: this.colors.table, label: 'Table' });
    if (types.has('view')) items.push({ color: this.colors.view, label: 'View' });
    if (types.has('cte')) items.push({ color: this.colors.cte, label: 'CTE' });
    if (types.has('subquery')) items.push({ color: this.colors.subquery, label: 'Subquery' });

    for (const item of items) {
      const el = document.createElement('div');
      el.className = 'legend-item';
      el.innerHTML = `<div class="legend-dot" style="background:${item.color}"></div>${item.label}`;
      legend.appendChild(el);
    }

    if (edgeTypes.has('join')) {
      const el = document.createElement('div');
      el.className = 'legend-item';
      el.innerHTML = `<div class="legend-line" style="background:#7a8aaa"></div>Join`;
      legend.appendChild(el);
    }

    if (edgeTypes.has('data_flow')) {
      const el = document.createElement('div');
      el.className = 'legend-item';
      el.innerHTML = `<div class="legend-line" style="background:#4a9eff"></div>Data Flow`;
      legend.appendChild(el);
    }

    if (edgeTypes.has('where_join')) {
      const el = document.createElement('div');
      el.className = 'legend-item';
      el.innerHTML = `<div class="legend-line dashed" style="background:repeating-linear-gradient(90deg,#f0c040 0px,#f0c040 4px,transparent 4px,transparent 7px)"></div>WHERE`;
      legend.appendChild(el);
    }

    this.container.appendChild(legend);
  }
}
