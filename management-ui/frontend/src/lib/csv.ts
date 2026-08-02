// Minimal RFC 4180-ish CSV encoding: quote a field only when it contains a
// comma, quote, or newline, doubling any embedded quotes -- no npm
// dependency needed for something this simple.
function csvField(value: string): string {
  if (/[",\n]/.test(value)) {
    return `"${value.replace(/"/g, '""')}"`;
  }
  return value;
}

export function rowsToCsv(headers: string[], rows: string[][]): string {
  const lines = [headers, ...rows].map((fields) => fields.map(csvField).join(","));
  return lines.join("\r\n");
}

// Triggers a browser download of `content` as a file named `filename`,
// without navigating away from the page.
export function downloadTextFile(filename: string, content: string, mimeType: string): void {
  const blob = new Blob([content], { type: mimeType });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}
