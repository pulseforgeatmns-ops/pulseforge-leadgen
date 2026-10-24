'use strict';

function formatIngestionReceipt({
  title,
  recordsExamined = null,
  committed = 0,
  held = 0,
  summaryLines = [],
  unresolvedCount = 0,
}) {
  const lines = [title];
  if (recordsExamined != null) {
    lines.push(`${recordsExamined} records examined.`);
  }
  for (const line of summaryLines) {
    if (line) lines.push(line);
  }
  if (held > 0) {
    lines.push(`${held} require review.`);
  } else if (unresolvedCount === 0) {
    lines.push('0 unresolved claims.');
  } else {
    lines.push(`${unresolvedCount} unresolved claims.`);
  }
  if (recordsExamined != null && held > 0) {
    lines.push('Safe records were committed independently.');
  }
  return lines.join(' ');
}

module.exports = {
  formatIngestionReceipt,
};
