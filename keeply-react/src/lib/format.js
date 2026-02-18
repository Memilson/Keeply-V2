export function normalizeEmail(email) {
  return (email || "").trim().toLowerCase();
}

export function formatLastSeen(value) {
  if (!value) return "-";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return String(value);
  return d.toLocaleString();
}