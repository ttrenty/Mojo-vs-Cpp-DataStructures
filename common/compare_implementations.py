from __future__ import annotations

import argparse
import difflib
import html
import json
import re
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC_PATH = ROOT / "common" / "implementation_compare_spec.json"
OUTPUT_DIR = ROOT / "results" / "processed"
HTML_OUTPUT = OUTPUT_DIR / "implementation_compare.html"
MD_OUTPUT = OUTPUT_DIR / "implementation_compare.md"
JSON_OUTPUT = OUTPUT_DIR / "implementation_compare.json"

CONTROL_KEYWORDS = {"if", "for", "while", "switch", "return", "catch"}
NORMALIZE_IGNORE = {
    "const",
    "constexpr",
    "static",
    "inline",
    "nodiscard",
    "noexcept",
    "mut",
    "read",
    "out",
    "self",
    "this",
    "std",
    "dsw",
    "uint64",
    "uint32",
    "size_t",
    "int",
    "bool",
    "float64",
    "string",
    "list",
    "optional",
    "span",
    "copyable",
    "movable",
    "implicitlycopyable",
    "return",
}
CPP_KEYWORDS = {
    "auto",
    "break",
    "case",
    "catch",
    "class",
    "const",
    "constexpr",
    "continue",
    "default",
    "delete",
    "else",
    "false",
    "for",
    "if",
    "inline",
    "namespace",
    "new",
    "noexcept",
    "nullptr",
    "override",
    "private",
    "protected",
    "public",
    "return",
    "static",
    "struct",
    "switch",
    "template",
    "this",
    "throw",
    "true",
    "try",
    "typename",
    "using",
    "virtual",
    "while",
}
CPP_TYPES = {
    "bool",
    "double",
    "float",
    "int",
    "size_t",
    "std::optional",
    "std::size_t",
    "std::span",
    "std::string",
    "std::string_view",
    "std::uint32_t",
    "std::uint64_t",
    "std::vector",
    "uint32_t",
    "uint64_t",
    "void",
}
MOJO_KEYWORDS = {
    "alias",
    "and",
    "def",
    "else",
    "False",
    "for",
    "from",
    "if",
    "import",
    "in",
    "let",
    "mut",
    "None",
    "not",
    "or",
    "out",
    "raises",
    "read",
    "ref",
    "return",
    "staticmethod",
    "struct",
    "True",
    "var",
    "while",
}
MOJO_TYPES = {
    "Bool",
    "Copyable",
    "Error",
    "Float64",
    "ImplicitlyCopyable",
    "ImmutOrigin",
    "Int",
    "List",
    "Movable",
    "Optional",
    "Span",
    "StaticConstantOrigin",
    "StaticString",
    "String",
    "TrivialRegisterPassable",
    "UInt32",
    "UInt64",
}
CPP_HIGHLIGHT_RE = re.compile(
    r"/\*[\s\S]*?\*/|//[^\n]*|"
    r'"(?:\\.|[^"\\])*"|'
    r"'(?:\\.|[^'\\])*'|"
    r"\[\[[^\]]+\]\]|"
    r"\b(?:0x[0-9A-Fa-f]+|\d+(?:\.\d+)?)\b|"
    r"[A-Za-z_~][A-Za-z0-9_:]*"
)
MOJO_HIGHLIGHT_RE = re.compile(
    r"#.*|"
    r'"""[\s\S]*?"""|'
    r'"(?:\\.|[^"\\])*"|'
    r"'(?:\\.|[^'\\])*'|"
    r"@[A-Za-z_][A-Za-z0-9_]*|"
    r"\b(?:0x[0-9A-Fa-f]+|\d+(?:\.\d+)?)\b|"
    r"[A-Za-z_][A-Za-z0-9_]*"
)


@dataclass
class SymbolBlock:
    name: str
    kind: str
    start_line: int
    end_line: int
    text: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a side-by-side comparison report for the C++ and Mojo implementations."
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Fail if helpers are missing, out of order, unexpectedly unmapped, or still using disallowed aliases.",
    )
    return parser.parse_args()


def load_spec() -> dict[str, object]:
    return json.loads(SPEC_PATH.read_text(encoding="utf-8"))


def count_indent(line: str) -> int:
    return len(line) - len(line.lstrip(" "))


def find_mojo_header_end(lines: list[str], start_index: int) -> int:
    paren_depth = 0
    bracket_depth = 0
    brace_depth = 0
    for index in range(start_index, len(lines)):
        line = lines[index]
        stripped = line.strip()
        paren_depth += line.count("(") - line.count(")")
        bracket_depth += line.count("[") - line.count("]")
        brace_depth += line.count("{") - line.count("}")
        if (
            stripped.endswith(":")
            and paren_depth <= 0
            and bracket_depth <= 0
            and brace_depth <= 0
        ):
            return index
    return start_index


def extract_cpp_block(lines: list[str], start_index: int) -> SymbolBlock:
    brace_depth = 0
    saw_open = False
    end_index = start_index
    for index in range(start_index, len(lines)):
        line = lines[index]
        brace_depth += line.count("{")
        if "{" in line:
            saw_open = True
        brace_depth -= line.count("}")
        end_index = index
        if saw_open and brace_depth <= 0:
            break
    text = "\n".join(lines[start_index : end_index + 1]).rstrip()
    return SymbolBlock("", "", start_index + 1, end_index + 1, text)


def has_cpp_body(lines: list[str], start_index: int) -> bool:
    for index in range(start_index, len(lines)):
        line = lines[index]
        if "{" in line:
            return True
        if ";" in line:
            return False
    return False


def parse_cpp_symbols(path: Path) -> tuple[dict[str, SymbolBlock], dict[str, SymbolBlock]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    function_blocks: dict[str, SymbolBlock] = {}
    type_blocks: dict[str, SymbolBlock] = {}

    type_pattern = re.compile(
        r"^\s*(?:class|struct)\s+(?:alignas\([^)]*\)\s+)?([A-Za-z_][A-Za-z0-9_]*)\b"
    )
    function_pattern = re.compile(
        r"^\s*(?:template\s*<[^>]+>\s*)?"
        r"(?:\[\[nodiscard\]\]\s+)?"
        r"(?:inline\s+)?(?:static\s+)?(?:constexpr\s+)?(?:virtual\s+)?"
        r"(?:[A-Za-z_~:<>0-9,\s*&]+\s+)?([A-Za-z_~][A-Za-z0-9_]*)\s*\("
    )

    for index, line in enumerate(lines):
        stripped = line.strip()
        type_match = type_pattern.match(line)
        if type_match:
            name = type_match.group(1)
            if name not in type_blocks:
                block = extract_cpp_block(lines, index)
                block.name = name
                block.kind = "type"
                type_blocks[name] = block

        if stripped.startswith(("class ", "struct ", "enum ", "using ", "typedef ", ":")):
            continue
        function_match = function_pattern.match(line)
        if not function_match:
            continue
        name = function_match.group(1)
        if name in CONTROL_KEYWORDS or name in function_blocks or name.endswith("_"):
            continue
        if not has_cpp_body(lines, index):
            continue
        block = extract_cpp_block(lines, index)
        block.name = name
        block.kind = "function"
        function_blocks[name] = block

    return function_blocks, type_blocks


def extract_mojo_block(lines: list[str], start_index: int, include_decorators: bool) -> SymbolBlock:
    block_start = start_index
    if include_decorators:
        while block_start > 0 and lines[block_start - 1].lstrip().startswith("@"):
            block_start -= 1

    base_line = lines[start_index]
    base_indent = count_indent(base_line)
    header_end = find_mojo_header_end(lines, start_index)
    end_index = header_end
    for index in range(header_end + 1, len(lines)):
        line = lines[index]
        stripped = line.strip()
        if not stripped:
            end_index = index
            continue
        indent = count_indent(line)
        if indent <= base_indent:
            break
        end_index = index
    text = "\n".join(lines[block_start : end_index + 1]).rstrip()
    return SymbolBlock("", "", block_start + 1, end_index + 1, text)


def parse_mojo_symbols(path: Path) -> tuple[dict[str, SymbolBlock], dict[str, SymbolBlock]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    function_blocks: dict[str, SymbolBlock] = {}
    type_blocks: dict[str, SymbolBlock] = {}

    type_pattern = re.compile(r"^\s*struct\s+([A-Za-z_][A-Za-z0-9_]*)\b")
    for index, line in enumerate(lines):
        type_match = type_pattern.match(line)
        if type_match:
            name = type_match.group(1)
            if name not in type_blocks:
                block = extract_mojo_block(lines, index, include_decorators=True)
                block.name = name
                block.kind = "type"
                type_blocks[name] = block

        function_match = re.match(
            r"^\s*def\s+([A-Za-z_][A-Za-z0-9_]*)\s*(?:\[.*\])?\s*\(",
            line,
        )
        if function_match:
            name = function_match.group(1)
            if name not in function_blocks:
                block = extract_mojo_block(lines, index, include_decorators=True)
                block.name = name
                block.kind = "function"
                function_blocks[name] = block

    return function_blocks, type_blocks


def normalize_code(text: str) -> str:
    cleaned = re.sub(r"//.*", "", text)
    cleaned = re.sub(r"#.*", "", cleaned)
    cleaned = cleaned.replace("this->", "")
    cleaned = cleaned.replace("self.", "")
    cleaned = cleaned.replace("std::", "")
    cleaned = cleaned.replace("dsw::", "")
    cleaned = re.sub(r"static_cast<[^>]+>\(", "(", cleaned)
    cleaned = re.sub(r"(UInt64|UInt32|Int|Float64)\(", "(", cleaned)
    tokens = re.findall(
        r"[A-Za-z_][A-Za-z0-9_]*|0x[0-9A-Fa-f]+|\d+|==|!=|<=|>=|&&|\|\||<<|>>|[+\-*/%&|^<>=]",
        cleaned,
    )
    normalized_tokens = [
        token.lower()
        for token in tokens
        if token.lower() not in NORMALIZE_IGNORE
    ]
    return " ".join(normalized_tokens)


def similarity_score(cpp_text: str, mojo_text: str) -> float:
    cpp_normalized = normalize_code(cpp_text)
    mojo_normalized = normalize_code(mojo_text)
    if not cpp_normalized and not mojo_normalized:
        return 1.0
    return difflib.SequenceMatcher(None, cpp_normalized, mojo_normalized).ratio()


def order_positions(
    pairs: list[dict[str, object]],
    blocks: dict[str, SymbolBlock],
    language_key: str,
) -> dict[str, int]:
    positions: dict[str, int] = {}
    order_index = 0
    for pair in pairs:
        if pair.get("enforce_order", True) is False:
            continue
        symbol_name = str(pair[language_key])
        block = blocks.get(symbol_name)
        if block is None:
            continue
        positions[str(pair["canonical"])] = order_index
        order_index += 1
    return positions


def expected_positions(pairs: list[dict[str, object]], present: set[str]) -> dict[str, int]:
    positions: dict[str, int] = {}
    order_index = 0
    for pair in pairs:
        canonical = str(pair["canonical"])
        if pair.get("enforce_order", True) is False:
            continue
        if canonical not in present:
            continue
        positions[canonical] = order_index
        order_index += 1
    return positions


def module_summary(module: dict[str, object]) -> dict[str, object]:
    cpp_path = ROOT / str(module["cpp_path"])
    mojo_path = ROOT / str(module["mojo_path"])
    cpp_functions, cpp_types = parse_cpp_symbols(cpp_path)
    mojo_functions, mojo_types = parse_mojo_symbols(mojo_path)

    pairs: list[dict[str, object]] = list(module["pairs"])
    pair_rows: list[dict[str, object]] = []
    present_canonicals: set[str] = set()
    for pair in pairs:
        kind = str(pair["kind"])
        cpp_blocks = cpp_functions if kind == "function" else cpp_types
        mojo_blocks = mojo_functions if kind == "function" else mojo_types
        cpp_name = str(pair["cpp"])
        mojo_name = str(pair["mojo"])
        cpp_block = cpp_blocks.get(cpp_name)
        mojo_block = mojo_blocks.get(mojo_name)
        if cpp_block and mojo_block:
            present_canonicals.add(str(pair["canonical"]))

        pair_rows.append(
            {
                "canonical": str(pair["canonical"]),
                "kind": kind,
                "cpp_name": cpp_name,
                "mojo_name": mojo_name,
                "cpp_block": cpp_block,
                "mojo_block": mojo_block,
                "enforce_order": bool(pair.get("enforce_order", True)),
            }
        )

    cpp_actual_positions = order_positions(pairs, cpp_functions, "cpp")
    mojo_actual_positions = order_positions(pairs, mojo_functions, "mojo")
    expected_order = expected_positions(pairs, present_canonicals)

    order_drift: list[str] = []
    for canonical, expected_position in expected_order.items():
        cpp_position = cpp_actual_positions.get(canonical)
        mojo_position = mojo_actual_positions.get(canonical)
        if cpp_position != expected_position or mojo_position != expected_position:
            order_drift.append(canonical)

    missing: list[str] = []
    for row in pair_rows:
        if row["cpp_block"] is None or row["mojo_block"] is None:
            missing.append(row["canonical"])

    compared_cpp_functions = {str(pair["cpp"]) for pair in pairs if pair["kind"] == "function"}
    compared_mojo_functions = {str(pair["mojo"]) for pair in pairs if pair["kind"] == "function"}
    unexpected_cpp = sorted(
        set(cpp_functions)
        - compared_cpp_functions
        - set(module["allowed_cpp_only_functions"])
    )
    unexpected_mojo = sorted(
        set(mojo_functions)
        - compared_mojo_functions
        - set(module["allowed_mojo_only_functions"])
    )

    all_cpp_symbols = set(cpp_functions) | set(cpp_types)
    all_mojo_symbols = set(mojo_functions) | set(mojo_types)
    disallowed_cpp = sorted(all_cpp_symbols & set(module["disallowed_cpp_symbols"]))
    disallowed_mojo = sorted(all_mojo_symbols & set(module["disallowed_mojo_symbols"]))

    for row in pair_rows:
        cpp_block = row["cpp_block"]
        mojo_block = row["mojo_block"]
        if cpp_block is None or mojo_block is None:
            row["status"] = "missing"
            row["similarity"] = None
            row["cpp_normalized"] = ""
            row["mojo_normalized"] = ""
            continue

        row["similarity"] = similarity_score(cpp_block.text, mojo_block.text)
        row["cpp_normalized"] = normalize_code(cpp_block.text)
        row["mojo_normalized"] = normalize_code(mojo_block.text)

        if row["canonical"] in order_drift:
            row["status"] = "order drift"
        elif row["cpp_name"] != row["mojo_name"]:
            row["status"] = "alias"
        elif row["similarity"] < 0.45:
            row["status"] = "manual review"
        else:
            row["status"] = "aligned"

    return {
        "name": module["name"],
        "cpp_path": str(module["cpp_path"]),
        "mojo_path": str(module["mojo_path"]),
        "notes": list(module["notes"]),
        "representation_differences": list(module["representation_differences"]),
        "pairs": pair_rows,
        "missing": missing,
        "order_drift": order_drift,
        "unexpected_cpp": unexpected_cpp,
        "unexpected_mojo": unexpected_mojo,
        "disallowed_cpp": disallowed_cpp,
        "disallowed_mojo": disallowed_mojo,
        "ok": not (missing or order_drift or unexpected_cpp or unexpected_mojo or disallowed_cpp or disallowed_mojo),
    }


def badge_class(status: str) -> str:
    return {
        "aligned": "ok",
        "alias": "warn",
        "manual review": "warn",
        "order drift": "bad",
        "missing": "bad",
    }[status]


def wrap_token(token: str, css_class: str) -> str:
    return f"<span class='{css_class}'>{html.escape(token)}</span>"


def token_class_for_identifier(token: str, language: str, source_text: str, token_end: int) -> str | None:
    keyword_set = CPP_KEYWORDS if language == "cpp" else MOJO_KEYWORDS
    type_set = CPP_TYPES if language == "cpp" else MOJO_TYPES
    if token in keyword_set:
        return "tok-keyword"
    if token in type_set:
        return "tok-type"
    if re.fullmatch(r"[A-Z][A-Z0-9_]*", token):
        return "tok-constant"
    if re.fullmatch(r"(?:[A-Z][A-Za-z0-9]*)+(?:::(?:[A-Z][A-Za-z0-9]*)+)*", token):
        return "tok-type"

    for char in source_text[token_end:]:
        if char.isspace():
            continue
        if char == "(":
            return "tok-call"
        break
    return None


def highlight_code(text: str, language: str) -> str:
    if language == "normalized":
        return html.escape(text)

    pattern = CPP_HIGHLIGHT_RE if language == "cpp" else MOJO_HIGHLIGHT_RE
    parts: list[str] = []
    cursor = 0
    for match in pattern.finditer(text):
        start, end = match.span()
        if start > cursor:
            parts.append(html.escape(text[cursor:start]))
        token = match.group(0)

        css_class: str | None = None
        if language == "cpp" and (token.startswith("//") or token.startswith("/*")):
            css_class = "tok-comment"
        elif language == "mojo" and token.startswith("#"):
            css_class = "tok-comment"
        elif token.startswith(('"', "'", '"""')):
            css_class = "tok-string"
        elif token.startswith("[[") or token.startswith("@"):
            css_class = "tok-decorator"
        elif re.fullmatch(r"(?:0x[0-9A-Fa-f]+|\d+(?:\.\d+)?)", token):
            css_class = "tok-number"
        else:
            css_class = token_class_for_identifier(token, language, text, end)

        if css_class is None:
            parts.append(html.escape(token))
        else:
            parts.append(wrap_token(token, css_class))
        cursor = end

    if cursor < len(text):
        parts.append(html.escape(text[cursor:]))
    return "".join(parts)


def render_code_block(text: str, language: str) -> str:
    return (
        f"<pre class='code-block {language}'><code>{highlight_code(text, language)}</code></pre>"
    )


def render_html(summary: list[dict[str, object]]) -> str:
    filter_options: list[str] = ["<option value='all'>All file pairs</option>"]
    for module in summary:
        module_name = str(module["name"])
        cpp_path = str(module["cpp_path"])
        mojo_path = str(module["mojo_path"])
        cpp_file = Path(cpp_path).name
        mojo_file = Path(mojo_path).name
        filter_options.append(
            "<option value='"
            + html.escape(module_name, quote=True)
            + "'>"
            + html.escape(f"{module_name}: {cpp_file} ↔ {mojo_file}")
            + "</option>"
        )

    rows: list[str] = [
        "<!doctype html>",
        "<html lang='en'>",
        "<head>",
        "<meta charset='utf-8'>",
        "<title>C++ / Mojo Implementation Comparison</title>",
        "<style>",
        "body{font-family:system-ui,-apple-system,sans-serif;margin:24px;color:#1f2933;background:#f7fafc;}",
        "h1,h2,h3{margin:0 0 12px 0;}",
        ".toolbar{position:sticky;top:0;z-index:10;background:#f7fafc;padding:12px 0 16px;margin-bottom:20px;}",
        ".toolbar-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;}",
        ".toolbar label{display:block;font-size:13px;font-weight:600;color:#334e68;}",
        ".toolbar select,.toolbar input{width:100%;margin-top:6px;padding:10px 12px;border-radius:10px;border:1px solid #bcccdc;background:white;font:inherit;color:inherit;box-sizing:border-box;}",
        ".legend{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px;margin-bottom:20px;}",
        ".module{background:white;border:1px solid #d9e2ec;border-radius:12px;padding:20px;margin-bottom:24px;box-shadow:0 1px 2px rgba(15,23,42,.05);}",
        ".summary{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px;margin:12px 0 20px;}",
        ".card{background:#f8fbff;border:1px solid #d9e2ec;border-radius:10px;padding:12px;}",
        ".badge{display:inline-block;padding:3px 8px;border-radius:999px;font-size:12px;font-weight:600;text-transform:uppercase;letter-spacing:.03em;}",
        ".badge.ok{background:#e3f9e5;color:#147d64;}",
        ".badge.warn{background:#fff3c4;color:#8d6e00;}",
        ".badge.bad{background:#ffe3e3;color:#c92a2a;}",
        ".badge.info{background:#d9e2ff;color:#486581;}",
        ".status-list{list-style:none;margin:8px 0 0 0;padding:0;}",
        ".status-list li{display:grid;grid-template-columns:auto 1fr auto;gap:10px;align-items:start;padding:8px 0;border-top:1px solid #e4ecf3;}",
        ".status-list li:first-child{border-top:none;padding-top:0;}",
        ".count{font-variant-numeric:tabular-nums;min-width:3ch;text-align:right;}",
        ".pair{border-top:1px solid #e4ecf3;padding-top:16px;margin-top:16px;}",
        ".meta{color:#52606d;font-size:14px;margin-bottom:10px;}",
        ".grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;}",
        ".panel{background:#fbfdff;border:1px solid #d9e2ec;border-radius:10px;padding:12px;}",
        ".panel.cpp-panel{border-top:3px solid #3b82f6;}",
        ".panel.mojo-panel{border-top:3px solid #f97316;}",
        ".panel.normalized-panel{border-top:3px solid #64748b;}",
        ".panel h4{margin:0 0 8px 0;font-size:14px;}",
        "pre{margin:0;white-space:pre-wrap;word-break:break-word;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px;line-height:1.5;}",
        ".code-block{padding:14px 16px;border-radius:10px;overflow:auto;tab-size:4;box-shadow:inset 0 1px 0 rgba(255,255,255,.04);}",
        ".code-block code{font-family:inherit;}",
        ".code-block.cpp{background:linear-gradient(180deg,#0f172a 0%,#132238 100%);color:#e5eef9;}",
        ".code-block.mojo{background:linear-gradient(180deg,#1f1720 0%,#2a1d22 100%);color:#fff1e6;}",
        ".code-block.normalized{background:linear-gradient(180deg,#111827 0%,#1f2937 100%);color:#e5e7eb;}",
        ".tok-comment{color:#94a3b8;font-style:italic;}",
        ".cpp .tok-keyword,.code-block.cpp .tok-keyword{color:#93c5fd;font-weight:700;}",
        ".mojo .tok-keyword,.code-block.mojo .tok-keyword{color:#fdba74;font-weight:700;}",
        ".tok-type{color:#86efac;font-weight:600;}",
        ".tok-call{color:#f9a8d4;}",
        ".tok-number{color:#fcd34d;}",
        ".tok-string{color:#c4f1be;}",
        ".tok-decorator{color:#67e8f9;font-weight:600;}",
        ".tok-constant{color:#d8b4fe;font-weight:600;}",
        "ul{margin:8px 0 0 20px;}",
        "details{margin-top:10px;}",
        "code{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;}",
        ".small{font-size:13px;color:#52606d;}",
        "@media (max-width: 1000px){.grid,.summary,.legend,.toolbar-grid{grid-template-columns:1fr;}}",
        "</style>",
        "</head>",
        "<body>",
        "<h1>C++ / Mojo Logic Comparison</h1>",
        "<p>This report compares the hand-written logic in the paired C++ and Mojo implementations, highlights intentional representation differences, and flags helper drift that could undermine a fair comparison.</p>",
        "<section class='toolbar card'>",
        "<div class='toolbar-grid'>",
        "<label>Module / file pair<select id='module-filter'>",
        *filter_options,
        "</select></label>",
        "<label>Path contains<input id='path-filter' type='text' placeholder='blocked_bloom, quotient_filter, ...'></label>",
        "<label>Status<select id='status-filter'><option value='all'>All modules</option><option value='pass'>Pass only</option><option value='check-required'>Check required only</option></select></label>",
        "<label>Sort modules<select id='sort-modules'><option value='spec'>Spec order</option><option value='name'>Module name</option><option value='cpp'>C++ file</option><option value='mojo'>Mojo file</option><option value='status'>Status</option></select></label>",
        "</div>",
        "</section>",
        "<section class='legend'>",
        "<div class='card'><strong>Status tags</strong><ul class='status-list'>",
        "<li><span class='badge ok'>aligned</span><span>both helpers are present, already use the same name, keep the expected order, and their normalized similarity stays above the review threshold.</span><span id='count-aligned' class='badge info count'>0</span></li>",
        "<li><span class='badge warn'>alias</span><span>the helpers are matched logically but still use different names across languages.</span><span id='count-alias' class='badge info count'>0</span></li>",
        "<li><span class='badge warn'>manual review</span><span>both helpers exist, but their normalized similarity is below the threshold, so the raw side-by-side code deserves a closer look.</span><span id='count-manual-review' class='badge info count'>0</span></li>",
        "<li><span class='badge bad'>order drift</span><span>the helper exists in both files, but its relative position no longer matches the agreed source-layout contract.</span><span id='count-order-drift' class='badge info count'>0</span></li>",
        "<li><span class='badge bad'>missing</span><span>one side of the helper pair was not found by the parser.</span><span id='count-missing' class='badge info count'>0</span></li>",
        "</ul></div>",
        "<div class='card'><strong>Similarity score</strong><p class='small'>The score is advisory only. It is computed by stripping comments, normalizing tokens such as <code>self.</code>, <code>this-&gt;</code>, <code>std::</code>, <code>dsw::</code>, and common type/qualifier words, then comparing the resulting token streams with Python's <code>difflib.SequenceMatcher</code>. A score of 1.000 means the normalized token sequences match exactly; the current report marks pairs below 0.450 as <code>manual review</code>.</p></div>",
        "</section>",
        "<div id='module-container'>",
    ]

    for index, module in enumerate(summary):
        module_name = str(module["name"])
        cpp_path = str(module["cpp_path"])
        mojo_path = str(module["mojo_path"])
        module_status = "pass" if module["ok"] else "check-required"
        rows.append(
            "<section class='module' data-index='"
            + str(index)
            + "' data-module='"
            + html.escape(module_name, quote=True)
            + "' data-cpp-path='"
            + html.escape(cpp_path, quote=True)
            + "' data-mojo-path='"
            + html.escape(mojo_path, quote=True)
            + "' data-status='"
            + module_status
            + "'><h2>"
            + html.escape(module_name)
            + "</h2>"
        )
        rows.append(
            f"<div class='meta'>C++: <code>{html.escape(cpp_path)}</code><br>Mojo: <code>{html.escape(mojo_path)}</code></div>"
        )
        rows.append("<div class='summary'>")
        rows.append(
            "<div class='card'><strong>Module status</strong><br>"
            + (
                "<span class='badge ok'>pass</span>"
                if module["ok"]
                else "<span class='badge bad'>check required</span>"
            )
            + "</div>"
        )
        rows.append(
            "<div class='card'><strong>Missing / order drift</strong><br>"
            + f"missing={len(module['missing'])}, order_drift={len(module['order_drift'])}</div>"
        )
        rows.append(
            "<div class='card'><strong>Unexpected helpers</strong><br>"
            + f"C++={len(module['unexpected_cpp'])}, Mojo={len(module['unexpected_mojo'])}</div>"
        )
        rows.append(
            "<div class='card'><strong>Disallowed aliases</strong><br>"
            + f"C++={len(module['disallowed_cpp'])}, Mojo={len(module['disallowed_mojo'])}</div>"
        )
        rows.append("</div>")

        if module["notes"]:
            rows.append("<div class='card'><strong>Notes</strong><ul>")
            for note in module["notes"]:
                rows.append(f"<li>{html.escape(note)}</li>")
            rows.append("</ul></div>")

        if module["representation_differences"]:
            rows.append("<div class='card'><strong>Allowed representation differences</strong><ul>")
            for difference in module["representation_differences"]:
                rows.append(f"<li>{html.escape(difference)}</li>")
            rows.append("</ul></div>")

        if module["missing"] or module["order_drift"] or module["unexpected_cpp"] or module["unexpected_mojo"] or module["disallowed_cpp"] or module["disallowed_mojo"]:
            rows.append("<div class='card'><strong>Module findings</strong><ul>")
            for name in module["missing"]:
                rows.append(f"<li>Missing helper pair: <code>{html.escape(name)}</code></li>")
            for name in module["order_drift"]:
                rows.append(f"<li>Order drift: <code>{html.escape(name)}</code></li>")
            for name in module["unexpected_cpp"]:
                rows.append(f"<li>Unexpected unmapped C++ helper: <code>{html.escape(name)}</code></li>")
            for name in module["unexpected_mojo"]:
                rows.append(f"<li>Unexpected unmapped Mojo helper: <code>{html.escape(name)}</code></li>")
            for name in module["disallowed_cpp"]:
                rows.append(f"<li>Disallowed C++ symbol still present: <code>{html.escape(name)}</code></li>")
            for name in module["disallowed_mojo"]:
                rows.append(f"<li>Disallowed Mojo symbol still present: <code>{html.escape(name)}</code></li>")
            rows.append("</ul></div>")

        for pair in module["pairs"]:
            cpp_block = pair["cpp_block"]
            mojo_block = pair["mojo_block"]
            rows.append(
                "<div class='pair' data-pair-status='"
                + html.escape(str(pair["status"]), quote=True)
                + "'>"
            )
            rows.append(
                f"<h3>{html.escape(pair['canonical'])} <span class='badge {badge_class(pair['status'])}'>{html.escape(pair['status'])}</span></h3>"
            )
            if pair["similarity"] is not None:
                rows.append(
                    f"<div class='meta'>Normalized similarity: {pair['similarity']:.3f}</div>"
                )
            rows.append("<div class='grid'>")
            rows.append("<div class='panel cpp-panel'>")
            rows.append("<h4>C++</h4>")
            if cpp_block is None:
                rows.append("<div class='meta'>Missing</div>")
            else:
                rows.append(
                    f"<div class='meta'><code>{html.escape(str(module['cpp_path']))}:{cpp_block.start_line}-{cpp_block.end_line}</code></div>"
                )
                rows.append(render_code_block(cpp_block.text, "cpp"))
            rows.append("</div>")
            rows.append("<div class='panel mojo-panel'>")
            rows.append("<h4>Mojo</h4>")
            if mojo_block is None:
                rows.append("<div class='meta'>Missing</div>")
            else:
                rows.append(
                    f"<div class='meta'><code>{html.escape(str(module['mojo_path']))}:{mojo_block.start_line}-{mojo_block.end_line}</code></div>"
                )
                rows.append(render_code_block(mojo_block.text, "mojo"))
            rows.append("</div></div>")

            if pair["cpp_block"] is not None and pair["mojo_block"] is not None:
                rows.append("<details><summary>Normalized logic view</summary>")
                rows.append("<div class='grid'>")
                rows.append("<div class='panel normalized-panel'><h4>C++ normalized</h4>")
                rows.append(render_code_block(str(pair["cpp_normalized"]), "normalized"))
                rows.append("</div>")
                rows.append("<div class='panel normalized-panel'><h4>Mojo normalized</h4>")
                rows.append(render_code_block(str(pair["mojo_normalized"]), "normalized"))
                rows.append("</div>")
                rows.append("</div></details>")
            rows.append("</div>")

        rows.append("</section>")

    rows.extend(
        [
            "</div>",
            "<script>",
            "const moduleContainer = document.getElementById('module-container');",
            "const moduleFilter = document.getElementById('module-filter');",
            "const pathFilter = document.getElementById('path-filter');",
            "const statusFilter = document.getElementById('status-filter');",
            "const sortModules = document.getElementById('sort-modules');",
            "const allModules = Array.from(moduleContainer.querySelectorAll('.module'));",
            "const statusCountTargets = {",
            "  'aligned': document.getElementById('count-aligned'),",
            "  'alias': document.getElementById('count-alias'),",
            "  'manual review': document.getElementById('count-manual-review'),",
            "  'order drift': document.getElementById('count-order-drift'),",
            "  'missing': document.getElementById('count-missing'),",
            "};",
            "function moduleSortValue(module, mode) {",
            "  if (mode === 'name') return module.dataset.module.toLowerCase();",
            "  if (mode === 'cpp') return module.dataset.cppPath.toLowerCase();",
            "  if (mode === 'mojo') return module.dataset.mojoPath.toLowerCase();",
            "  if (mode === 'status') return (module.dataset.status === 'check-required' ? '0' : '1') + module.dataset.module.toLowerCase();",
            "  return String(module.dataset.index).padStart(6, '0');",
            "}",
            "function updateStatusCounts(visibleModules) {",
            "  const counts = {",
            "    'aligned': 0,",
            "    'alias': 0,",
            "    'manual review': 0,",
            "    'order drift': 0,",
            "    'missing': 0,",
            "  };",
            "  for (const module of visibleModules) {",
            "    for (const pair of module.querySelectorAll('.pair')) {",
            "      const status = pair.dataset.pairStatus;",
            "      if (Object.prototype.hasOwnProperty.call(counts, status)) counts[status] += 1;",
            "    }",
            "  }",
            "  for (const [status, target] of Object.entries(statusCountTargets)) target.textContent = String(counts[status]);",
            "}",
            "function applyModuleView() {",
            "  const moduleChoice = moduleFilter.value;",
            "  const pathQuery = pathFilter.value.trim().toLowerCase();",
            "  const statusChoice = statusFilter.value;",
            "  const sortChoice = sortModules.value;",
            "  const visible = [];",
            "  for (const module of allModules) {",
            "    const matchesModule = moduleChoice === 'all' || module.dataset.module === moduleChoice;",
            "    const matchesPath = !pathQuery || module.dataset.cppPath.toLowerCase().includes(pathQuery) || module.dataset.mojoPath.toLowerCase().includes(pathQuery) || module.dataset.module.toLowerCase().includes(pathQuery);",
            "    const matchesStatus = statusChoice === 'all' || module.dataset.status === statusChoice;",
            "    const show = matchesModule && matchesPath && matchesStatus;",
            "    module.style.display = show ? '' : 'none';",
            "    if (show) visible.push(module);",
            "  }",
            "  visible.sort((left, right) => moduleSortValue(left, sortChoice).localeCompare(moduleSortValue(right, sortChoice)));",
            "  for (const module of visible) moduleContainer.appendChild(module);",
            "  updateStatusCounts(visible);",
            "}",
            "moduleFilter.addEventListener('change', applyModuleView);",
            "pathFilter.addEventListener('input', applyModuleView);",
            "statusFilter.addEventListener('change', applyModuleView);",
            "sortModules.addEventListener('change', applyModuleView);",
            "applyModuleView();",
            "</script>",
            "</body>",
            "</html>",
        ]
    )
    return "\n".join(rows) + "\n"


def render_markdown(summary: list[dict[str, object]]) -> str:
    lines = ["# C++ / Mojo Logic Comparison", ""]
    for module in summary:
        lines.append(f"## {module['name']}")
        lines.append("")
        lines.append(f"- C++: `{module['cpp_path']}`")
        lines.append(f"- Mojo: `{module['mojo_path']}`")
        lines.append(f"- Status: `{'pass' if module['ok'] else 'check required'}`")
        if module["notes"]:
            lines.append("- Notes:")
            for note in module["notes"]:
                lines.append(f"  - {note}")
        if module["representation_differences"]:
            lines.append("- Allowed representation differences:")
            for difference in module["representation_differences"]:
                lines.append(f"  - {difference}")
        if module["missing"] or module["order_drift"] or module["unexpected_cpp"] or module["unexpected_mojo"] or module["disallowed_cpp"] or module["disallowed_mojo"]:
            lines.append("- Findings:")
            for name in module["missing"]:
                lines.append(f"  - Missing helper pair: `{name}`")
            for name in module["order_drift"]:
                lines.append(f"  - Order drift: `{name}`")
            for name in module["unexpected_cpp"]:
                lines.append(f"  - Unexpected unmapped C++ helper: `{name}`")
            for name in module["unexpected_mojo"]:
                lines.append(f"  - Unexpected unmapped Mojo helper: `{name}`")
            for name in module["disallowed_cpp"]:
                lines.append(f"  - Disallowed C++ symbol still present: `{name}`")
            for name in module["disallowed_mojo"]:
                lines.append(f"  - Disallowed Mojo symbol still present: `{name}`")
        lines.append("")
        lines.append("| Canonical | Status | Similarity |")
        lines.append("| --- | --- | ---: |")
        for pair in module["pairs"]:
            similarity = "" if pair["similarity"] is None else f"{pair['similarity']:.3f}"
            lines.append(
                f"| `{pair['canonical']}` | `{pair['status']}` | {similarity} |"
            )
        lines.append("")
    return "\n".join(lines) + "\n"


def write_outputs(summary: list[dict[str, object]]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    serializable = []
    for module in summary:
        module_copy = dict(module)
        module_copy["pairs"] = []
        for pair in module["pairs"]:
            module_copy["pairs"].append(
                {
                    "canonical": pair["canonical"],
                    "kind": pair["kind"],
                    "cpp_name": pair["cpp_name"],
                    "mojo_name": pair["mojo_name"],
                    "status": pair["status"],
                    "similarity": pair["similarity"],
                    "cpp_location": None
                    if pair["cpp_block"] is None
                    else {
                        "path": module["cpp_path"],
                        "start_line": pair["cpp_block"].start_line,
                        "end_line": pair["cpp_block"].end_line,
                    },
                    "mojo_location": None
                    if pair["mojo_block"] is None
                    else {
                        "path": module["mojo_path"],
                        "start_line": pair["mojo_block"].start_line,
                        "end_line": pair["mojo_block"].end_line,
                    },
                }
            )
        serializable.append(module_copy)

    HTML_OUTPUT.write_text(render_html(summary), encoding="utf-8")
    MD_OUTPUT.write_text(render_markdown(summary), encoding="utf-8")
    JSON_OUTPUT.write_text(json.dumps({"modules": serializable}, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    spec = load_spec()
    summary = [module_summary(module) for module in spec["modules"]]
    write_outputs(summary)

    failures: list[str] = []
    for module in summary:
        for name in module["missing"]:
            failures.append(f"{module['name']}: missing helper pair {name}")
        for name in module["order_drift"]:
            failures.append(f"{module['name']}: helper order drift for {name}")
        for name in module["unexpected_cpp"]:
            failures.append(f"{module['name']}: unexpected C++ helper {name}")
        for name in module["unexpected_mojo"]:
            failures.append(f"{module['name']}: unexpected Mojo helper {name}")
        for name in module["disallowed_cpp"]:
            failures.append(f"{module['name']}: disallowed C++ symbol {name}")
        for name in module["disallowed_mojo"]:
            failures.append(f"{module['name']}: disallowed Mojo symbol {name}")

    print(f"Wrote comparison artifacts to {OUTPUT_DIR}")
    if args.check and failures:
        raise SystemExit("\n".join(failures))


if __name__ == "__main__":
    main()
