'use strict';

(function () {
  function normalizeDueDate(value) {
    if (value == null || value === '') return null;
    if (value instanceof Date) {
      if (Number.isNaN(value.getTime())) return null;
      return value.toISOString().slice(0, 10);
    }
    const match = String(value).match(/^(\d{4}-\d{2}-\d{2})/);
    return match ? match[1] : null;
  }

  function formatDueDateLabel(value) {
    const dateOnly = normalizeDueDate(value);
    if (!dateOnly) return '';
    const parsed = new Date(`${dateOnly}T12:00:00`);
    if (Number.isNaN(parsed.getTime())) return '';
    return parsed.toLocaleDateString(undefined, {
      weekday: 'short',
      month: 'short',
      day: 'numeric',
    });
  }

  window.AoQueueFormat = {
    normalizeDueDate,
    formatDueDateLabel,
  };
}());
