// A small TOML parser (TOML 1.0, with the newlines and the trailing commas that TOML 1.1
// permits in inline tables) that records where each key and each string is. The manifest
// parsers for pyproject.toml and Cargo.toml use it. This module has no `vscode` import.
//
// The parser keeps only the values that the manifest parsers read: tables, arrays, and
// strings. It does not check the TOML rules for redefined keys and tables, so that a file
// that a strict parser refuses for a small fault still gives its dependencies.

// A place in the text. The line and the columns are 0-based and count UTF-16 code units, as
// VS Code positions do.
export interface TomlPosition {
  line: number;
  startCharacter: number;
  endCharacter: number;
}

export type TomlValue = TomlTable | TomlArray | TomlString | { kind: "other" };

export interface TomlTable {
  kind: "table";
  entries: Map<string, TomlEntry>;
}

export interface TomlArray {
  kind: "array";
  items: TomlValue[];
}

// A string value. `position` covers the string with its quotes. `raw` is the source text
// between the quotes, which differs from `value` only when the string has escapes.
export interface TomlString {
  kind: "string";
  value: string;
  raw: string;
  position: TomlPosition;
}

// A key and its value. `key` is the position of the last part of the key where the key
// first occurs: in a key-value pair, or in a table header.
export interface TomlEntry {
  key: TomlPosition;
  value: TomlValue;
}

interface KeyPart {
  name: string;
  start: number;
  end: number;
}

// Parse a TOML document. Text with a syntax error throws a SyntaxError.
export function parseToml(text: string): TomlTable {
  return new TomlScanner(text).parseDocument();
}

// Return the table at a path of keys, or undefined. An array of tables gives its last table.
export function tableAt(table: TomlTable | undefined, ...path: string[]): TomlTable | undefined {
  let current: TomlValue | undefined = table;
  for (const name of path) {
    if (current?.kind !== "table") return undefined;
    current = current.entries.get(name)?.value;
  }
  return current?.kind === "table" ? current : undefined;
}

const BARE_KEY = /[A-Za-z0-9_-]+/y;
const SCALAR =
  /(?:true|false|[+-]?(?:inf|nan)|[+-]?[0-9][0-9A-Za-z_+\-.:]*(?:[ T][0-9]{2}:[0-9A-Za-z_+\-.:]*)?)/y;
const ESCAPES: Record<string, string> = {
  b: "\b",
  t: "\t",
  n: "\n",
  f: "\f",
  r: "\r",
  e: "\u001b",
  '"': '"',
  "\\": "\\",
};

class TomlScanner {
  private readonly text: string;
  private readonly lineStarts: number[] = [0];
  private offset = 0;

  constructor(text: string) {
    this.text = text;
    for (let i = 0; i < text.length; i++) if (text[i] === "\n") this.lineStarts.push(i + 1);
  }

  parseDocument(): TomlTable {
    const root = newTable();
    let current = root;
    if (this.text.charCodeAt(0) === 0xfeff) this.offset = 1;
    for (;;) {
      this.skipBlankLines();
      if (this.offset >= this.text.length) return root;
      if (this.text[this.offset] === "[") current = this.parseHeader(root);
      else this.parseKeyValue(current);
      this.expectLineEnd();
    }
  }

  // Parse `[a.b]` or `[[a.b]]` and return the table that the next key-value pairs go into.
  private parseHeader(root: TomlTable): TomlTable {
    const isArray = this.text.startsWith("[[", this.offset);
    this.offset += isArray ? 2 : 1;
    const parts = this.parseKey();
    this.expect(isArray ? "]]" : "]");
    const parent = this.descend(root, parts.slice(0, -1));
    const last = parts[parts.length - 1];
    const entry = parent.entries.get(last.name);
    if (isArray) {
      const table = newTable();
      if (!entry) {
        parent.entries.set(last.name, { key: this.position(last.start, last.end), value: { kind: "array", items: [table] } });
      } else if (entry.value.kind === "array") {
        entry.value.items.push(table);
      } else {
        this.fail();
      }
      return table;
    }
    if (!entry) {
      const table = newTable();
      parent.entries.set(last.name, { key: this.position(last.start, last.end), value: table });
      return table;
    }
    if (entry.value.kind !== "table") this.fail();
    return entry.value;
  }

  private parseKeyValue(table: TomlTable): void {
    const parts = this.parseKey();
    this.skipSpace();
    this.expect("=");
    this.skipSpace();
    const value = this.parseValue();
    const parent = this.descend(table, parts.slice(0, -1));
    const last = parts[parts.length - 1];
    parent.entries.set(last.name, { key: this.position(last.start, last.end), value });
  }

  // Return the table at the key parts below `table`, and make the tables that do not exist.
  private descend(table: TomlTable, parts: KeyPart[]): TomlTable {
    let current = table;
    for (const part of parts) {
      let entry = current.entries.get(part.name);
      if (!entry) {
        entry = { key: this.position(part.start, part.end), value: newTable() };
        current.entries.set(part.name, entry);
      }
      let value = entry.value;
      if (value.kind === "array") value = value.items[value.items.length - 1];
      if (value?.kind !== "table") this.fail();
      current = value;
    }
    return current;
  }

  // Parse a dotted key, for example `a."b.c".'d'`.
  private parseKey(): KeyPart[] {
    const parts: KeyPart[] = [];
    for (;;) {
      this.skipSpace();
      const start = this.offset;
      const char = this.text[this.offset];
      let name: string;
      if (char === '"') name = this.parseBasicString();
      else if (char === "'") name = this.parseLiteralString();
      else name = this.match(BARE_KEY) ?? this.fail();
      parts.push({ name, start, end: this.offset });
      this.skipSpace();
      if (this.text[this.offset] !== ".") return parts;
      this.offset++;
    }
  }

  private parseValue(): TomlValue {
    const start = this.offset;
    const char = this.text[this.offset];
    if (char === "[") return this.parseArray();
    if (char === "{") return this.parseInlineTable();
    if (char === '"' || char === "'") {
      const multiLine = this.text.startsWith(char.repeat(3), this.offset);
      let value: string;
      if (multiLine) value = char === '"' ? this.parseMultiLineBasic() : this.parseMultiLineLiteral();
      else value = char === '"' ? this.parseBasicString() : this.parseLiteralString();
      const quote = multiLine ? 3 : 1;
      return {
        kind: "string",
        value,
        raw: this.text.slice(start + quote, this.offset - quote),
        position: this.position(start, this.offset),
      };
    }
    if (this.match(SCALAR) === undefined) this.fail();
    return { kind: "other" };
  }

  private parseArray(): TomlArray {
    const items: TomlValue[] = [];
    this.offset++;
    for (;;) {
      this.skipBlankLines();
      if (this.text[this.offset] === "]") break;
      items.push(this.parseValue());
      this.skipBlankLines();
      if (this.text[this.offset] !== ",") break;
      this.offset++;
    }
    this.expect("]");
    return { kind: "array", items };
  }

  private parseInlineTable(): TomlTable {
    const table = newTable();
    this.offset++;
    for (;;) {
      this.skipBlankLines();
      if (this.text[this.offset] === "}") break;
      this.parseKeyValue(table);
      this.skipBlankLines();
      if (this.text[this.offset] !== ",") break;
      this.offset++;
    }
    this.expect("}");
    return table;
  }

  private parseBasicString(): string {
    this.offset++;
    let value = "";
    for (;;) {
      const char = this.text[this.offset];
      if (char === undefined || char === "\n" || char === "\r") this.fail();
      if (char === '"') {
        this.offset++;
        return value;
      }
      if (char === "\\") value += this.parseEscape();
      else {
        value += char;
        this.offset++;
      }
    }
  }

  private parseMultiLineBasic(): string {
    this.offset += 3;
    this.skipNewline();
    let value = "";
    for (;;) {
      if (this.offset >= this.text.length) this.fail();
      if (this.text.startsWith('"""', this.offset)) {
        // Up to two quotes can come just before the closing quotes.
        let end = this.offset + 3;
        while (this.text[end] === '"' && end - this.offset < 5) end++;
        value += '"'.repeat(end - this.offset - 3);
        this.offset = end;
        return value;
      }
      const char = this.text[this.offset];
      if (char === "\\") {
        const rest = /\\[ \t]*\r?\n[ \t\r\n]*/y;
        rest.lastIndex = this.offset;
        if (rest.test(this.text)) this.offset = rest.lastIndex;
        else value += this.parseEscape();
      } else {
        value += char;
        this.offset++;
      }
    }
  }

  private parseLiteralString(): string {
    const end = this.text.indexOf("'", this.offset + 1);
    if (end < 0) this.fail();
    const value = this.text.slice(this.offset + 1, end);
    if (/[\r\n]/.test(value)) this.fail();
    this.offset = end + 1;
    return value;
  }

  private parseMultiLineLiteral(): string {
    this.offset += 3;
    this.skipNewline();
    const end = this.text.indexOf("'''", this.offset);
    if (end < 0) this.fail();
    let close = end + 3;
    while (this.text[close] === "'" && close - end < 5) close++;
    const value = this.text.slice(this.offset, close - 3);
    this.offset = close;
    return value;
  }

  private parseEscape(): string {
    const code = this.text[this.offset + 1];
    if (code in ESCAPES) {
      this.offset += 2;
      return ESCAPES[code];
    }
    const digits = code === "x" ? 2 : code === "u" ? 4 : code === "U" ? 8 : 0;
    const hex = this.text.slice(this.offset + 2, this.offset + 2 + digits);
    if (digits === 0 || !/^[0-9A-Fa-f]+$/.test(hex) || hex.length !== digits) this.fail();
    this.offset += 2 + digits;
    return String.fromCodePoint(parseInt(hex, 16));
  }

  // Skip spaces, tabs, comments, and line ends.
  private skipBlankLines(): void {
    for (;;) {
      this.skipSpace();
      const char = this.text[this.offset];
      if (char === "#") this.skipComment();
      else if (char === "\n" || char === "\r") this.offset++;
      else return;
    }
  }

  private expectLineEnd(): void {
    this.skipSpace();
    if (this.text[this.offset] === "#") this.skipComment();
    if (this.offset >= this.text.length) return;
    if (!this.skipNewline()) this.fail();
  }

  private skipNewline(): boolean {
    if (this.text.startsWith("\r\n", this.offset)) this.offset += 2;
    else if (this.text[this.offset] === "\n") this.offset++;
    else return false;
    return true;
  }

  private skipComment(): void {
    while (this.offset < this.text.length && this.text[this.offset] !== "\n") this.offset++;
  }

  private skipSpace(): void {
    while (this.text[this.offset] === " " || this.text[this.offset] === "\t") this.offset++;
  }

  private match(pattern: RegExp): string | undefined {
    pattern.lastIndex = this.offset;
    const found = pattern.exec(this.text);
    if (!found) return undefined;
    this.offset = pattern.lastIndex;
    return found[0];
  }

  private expect(token: string): void {
    if (!this.text.startsWith(token, this.offset)) this.fail();
    this.offset += token.length;
  }

  // The position of the text from `start` to `end`. A range over more than one line ends at
  // the end of its first line.
  private position(start: number, end: number): TomlPosition {
    let low = 0;
    let high = this.lineStarts.length - 1;
    while (low < high) {
      const middle = Math.ceil((low + high) / 2);
      if (this.lineStarts[middle] <= start) low = middle;
      else high = middle - 1;
    }
    const lineStart = this.lineStarts[low];
    const lineEnd = low + 1 < this.lineStarts.length ? this.lineStarts[low + 1] - 1 : this.text.length;
    return { line: low, startCharacter: start - lineStart, endCharacter: Math.min(end, lineEnd) - lineStart };
  }

  private fail(): never {
    throw new SyntaxError(`TOML syntax error at offset ${this.offset}`);
  }
}

function newTable(): TomlTable {
  return { kind: "table", entries: new Map() };
}
