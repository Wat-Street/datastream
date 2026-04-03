/**
 * format an ISO timestamp string for display.
 * @param {string} isoString
 * @returns {string}
 */
export function formatTimestamp(isoString) {
  const d = new Date(isoString);
  return d.toLocaleDateString('en-US', {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
  });
}

/**
 * convert a Date to YYYY-MM-DD string for API queries and date inputs.
 * @param {Date} date
 * @returns {string}
 */
export function toISODate(date) {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, '0');
  const d = String(date.getDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

/**
 * return default date range: last 5 years as { start, end } strings.
 * wide range so the initial fetch is likely to find existing data.
 * @returns {{start: string, end: string}}
 */
export function defaultDateRange() {
  const end = new Date();
  const start = new Date();
  start.setFullYear(start.getFullYear() - 5);
  return { start: toISODate(start), end: toISODate(end) };
}

/**
 * produce syntax-highlighted HTML for a JSON-serializable value.
 * wraps tokens in spans with css classes for coloring.
 * @param {any} value
 * @param {number} indent - current indentation level
 * @returns {string}
 */
export function highlightJson(value, indent = 0) {
  const pad = '  '.repeat(indent);
  const padInner = '  '.repeat(indent + 1);

  if (value === null) {
    return `<span class="json-null">null</span>`;
  }

  if (typeof value === 'boolean') {
    return `<span class="json-boolean">${value}</span>`;
  }

  if (typeof value === 'number') {
    return `<span class="json-number">${value}</span>`;
  }

  if (typeof value === 'string') {
    const escaped = value
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
    return `<span class="json-string">"${escaped}"</span>`;
  }

  if (Array.isArray(value)) {
    if (value.length === 0) return '[]';
    const items = value
      .map((v) => `${padInner}${highlightJson(v, indent + 1)}`)
      .join(',\n');
    return `[\n${items}\n${pad}]`;
  }

  // object
  const keys = Object.keys(value);
  if (keys.length === 0) return '{}';
  const entries = keys
    .map((k) => {
      const keyHtml = `<span class="json-key">"${k}"</span>`;
      const valHtml = highlightJson(value[k], indent + 1);
      return `${padInner}${keyHtml}: ${valHtml}`;
    })
    .join(',\n');
  return `{\n${entries}\n${pad}}`;
}
