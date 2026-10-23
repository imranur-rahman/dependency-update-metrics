// The manifest parsers of the core module. This module has no `vscode` import, so that
// `node --test` tests it directly.

// The ecosystems of the Tool. The manifest parsers read npm only; #28 adds the parsers for
// "pypi" and "cargo". A Comparison takes all three.
export type Ecosystem = "npm" | "pypi" | "cargo";

// One direct dependency in a manifest. The line and the columns are 0-based and count
// UTF-16 code units, as VS Code positions do. The columns cover the name with its quotes.
export interface ManifestDependency {
  ecosystem: Ecosystem;
  name: string;
  section: string;
  line: number;
  startCharacter: number;
  endCharacter: number;
}

// A parser takes the text of one manifest and returns its direct dependencies.
export type ManifestParser = (text: string) => ManifestDependency[];

// The parser for each manifest file name. #28 adds pyproject.toml and Cargo.toml.
const PARSERS: Record<string, ManifestParser> = {
  "package.json": parsePackageJson,
};

// Return the direct dependencies of a manifest, in the order of the text. A file that is not
// a known manifest gives no dependencies.
export function parseManifest(fileName: string, text: string): ManifestDependency[] {
  const parser = PARSERS[baseName(fileName)];
  return parser ? parser(text) : [];
}

// Return true when the file name is a manifest that the Extension reads.
export function isManifest(fileName: string): boolean {
  return baseName(fileName) in PARSERS;
}

function baseName(fileName: string): string {
  return fileName.split(/[\\/]/).pop() ?? "";
}

// The sections of package.json that hold direct dependencies. bundleDependencies is a list
// of names that are also in the other sections, so the parser skips it.
const NPM_SECTIONS = ["dependencies", "devDependencies", "peerDependencies", "optionalDependencies"];

// Parse package.json. Text with a syntax error gives no dependencies.
export function parsePackageJson(text: string): ManifestDependency[] {
  let root: JsonValue;
  try {
    root = new JsonScanner(text).parseDocument();
  } catch {
    return [];
  }
  if (root.kind !== "object") return [];
  const lines = lineStarts(text);
  const deps: ManifestDependency[] = [];
  for (const member of root.members) {
    if (!NPM_SECTIONS.includes(member.key) || member.value.kind !== "object") continue;
    for (const dep of member.value.members) {
      if (dep.value.kind !== "string") continue;
      const start = position(lines, dep.keyStart);
      deps.push({
        ecosystem: "npm",
        name: dep.key,
        section: member.key,
        line: start.line,
        startCharacter: start.character,
        endCharacter: position(lines, dep.keyEnd).character,
      });
    }
  }
  return deps;
}

// The offset at which each line starts. A line ends at "\n", so "\r\n" counts as one end.
function lineStarts(text: string): number[] {
  const starts = [0];
  for (let i = 0; i < text.length; i++) if (text[i] === "\n") starts.push(i + 1);
  return starts;
}

function position(starts: number[], offset: number): { line: number; character: number } {
  let low = 0;
  let high = starts.length - 1;
  while (low < high) {
    const middle = Math.ceil((low + high) / 2);
    if (starts[middle] <= offset) low = middle;
    else high = middle - 1;
  }
  return { line: low, character: offset - starts[low] };
}

// A JSON value with the offsets of the object keys. The parser needs only objects and
// strings, so it keeps no other values.
type JsonValue =
  | { kind: "object"; members: JsonMember[] }
  | { kind: "string"; value: string }
  | { kind: "other" };

interface JsonMember {
  key: string;
  keyStart: number;
  keyEnd: number;
  value: JsonValue;
}

// A strict JSON parser (RFC 8259) that records where each object key is.
class JsonScanner {
  private readonly text: string;
  private offset = 0;

  constructor(text: string) {
    this.text = text;
  }

  parseDocument(): JsonValue {
    if (this.text.charCodeAt(0) === 0xfeff) this.offset = 1;
    const value = this.parseValue();
    this.skipSpace();
    if (this.offset !== this.text.length) this.fail();
    return value;
  }

  private parseValue(): JsonValue {
    this.skipSpace();
    const char = this.text[this.offset];
    if (char === "{") return this.parseObject();
    if (char === "[") return this.parseArray();
    if (char === '"') return { kind: "string", value: this.parseString() };
    const literal = /^(?:true|false|null|-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?)/.exec(
      this.text.slice(this.offset, this.offset + 400),
    );
    if (!literal) this.fail();
    this.offset += literal[0].length;
    return { kind: "other" };
  }

  private parseObject(): JsonValue {
    const members: JsonMember[] = [];
    this.offset++;
    this.skipSpace();
    if (this.text[this.offset] === "}") {
      this.offset++;
      return { kind: "object", members };
    }
    for (;;) {
      this.skipSpace();
      if (this.text[this.offset] !== '"') this.fail();
      const keyStart = this.offset;
      const key = this.parseString();
      const keyEnd = this.offset;
      this.skipSpace();
      this.expect(":");
      members.push({ key, keyStart, keyEnd, value: this.parseValue() });
      this.skipSpace();
      if (this.text[this.offset] === "}") {
        this.offset++;
        return { kind: "object", members };
      }
      this.expect(",");
    }
  }

  private parseArray(): JsonValue {
    this.offset++;
    this.skipSpace();
    if (this.text[this.offset] === "]") {
      this.offset++;
      return { kind: "other" };
    }
    for (;;) {
      this.parseValue();
      this.skipSpace();
      if (this.text[this.offset] === "]") {
        this.offset++;
        return { kind: "other" };
      }
      this.expect(",");
    }
  }

  private parseString(): string {
    const match = /^"(?:[^"\\\u0000-\u001f]|\\(?:["\\/bfnrt]|u[0-9a-fA-F]{4}))*"/.exec(
      this.text.slice(this.offset),
    );
    if (!match) this.fail();
    this.offset += match[0].length;
    return JSON.parse(match[0]) as string;
  }

  private expect(char: string): void {
    if (this.text[this.offset] !== char) this.fail();
    this.offset++;
  }

  private skipSpace(): void {
    while (/[ \t\r\n]/.test(this.text[this.offset] ?? "")) this.offset++;
  }

  private fail(): never {
    throw new SyntaxError(`JSON syntax error at offset ${this.offset}`);
  }
}
