"""Generate UI parameter metadata from the official OKX Chinese documentation snapshot."""
import argparse
import json
import re
from pathlib import Path

from bs4 import BeautifulSoup

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("html", type=Path)
parser.add_argument("catalog", type=Path)
parser.add_argument("--fallback", type=Path)
args = parser.parse_args()
soup = BeautifulSoup(args.html.read_text(), "html.parser")
fallback = BeautifulSoup(args.fallback.read_text(), "html.parser") if args.fallback else None
routes = json.loads(args.catalog.read_text())
labels = {}
count = 0
for route in routes:
    if route.get("detail_level") != "dedicated HTTP Request section":
        route.pop("inputSchema", None)
        route["formStatus"] = "reference"
        continue
    anchor = route["source"].split("#", 1)[1]
    heading = soup.find(id=anchor)
    if heading is None and fallback:
        heading = fallback.find(id=anchor)
    if heading is None:
        route["formStatus"] = "reference"
        continue
    route["displayTitle"] = re.sub(r"^(GET|POST)\s*[/：:]\s*", "", heading.get_text(" ", strip=True)).strip()
    level = int(heading.name[1])
    nodes = []
    for node in heading.next_siblings:
        if getattr(node, "name", None) and re.fullmatch(r"h[1-6]", node.name) and int(node.name[1]) <= level:
            break
        nodes.append(node)
    text = " ".join(n.get_text(" ", strip=True) for n in nodes if hasattr(n, "get_text"))
    if f"{route['method']} {route['path']}" not in text:
        route["formStatus"] = "reference"
        continue
    table = None
    request = False
    for node in nodes:
        if getattr(node, "name", None) in ("h4", "h5", "h6"):
            request = "请求参数" in node.get_text() or "Request Parameters" in node.get_text()
        if request and getattr(node, "name", None) == "table":
            table = node
            break
    if table is None and fallback:
        english = fallback.find(id=anchor + "-request-parameters")
        if english:
            candidate = english.find_next_sibling()
            if candidate and candidate.name == "table":
                table = candidate
    if table is None:
        route["formStatus"] = "no_parameters" if re.search(r"请求参数\s*(无|None)|Request Parameters\s*None", text, re.I) else "reference"
        if route["formStatus"] == "no_parameters":
            route["inputSchema"] = {"type": "object", "properties": {}}
        continue
    schema = {"type": "object", "properties": {}, "required": []}
    parents = {0: schema}
    for row in table.select("tbody tr"):
        cells = row.find_all("td", recursive=False)
        if len(cells) < 4:
            continue
        raw, kind, required, description = [c.get_text(" ", strip=True) for c in cells[:4]]
        depth = len(re.match(r"^[>\s]*", raw).group().replace(" ", ""))
        name = re.sub(r"^[>\s]+", "", raw).strip()
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", name):
            continue
        owner = parents.get(depth)
        if owner is None:
            continue
        typ = "array" if "array" in kind.lower() else "object" if "object" in kind.lower() else "boolean" if "boolean" in kind.lower() else "integer" if "integer" in kind.lower() else "number" if "number" in kind.lower() else "string"
        field = {"type": typ, "description": description, "title": re.split(r"[，。；：\n]| 如| 例如", description, 1)[0][:20] or name}
        labels.setdefault(name, field["title"])
        owner.setdefault("properties", {})[name] = field
        if required in ("是", "Yes"):
            owner.setdefault("required", []).append(name)
        if typ == "array":
            field["items"] = {"type": "object", "properties": {}} if "object" in kind.lower() else {"type": "string"}
            parents[depth + 1] = field["items"]
        elif typ == "object":
            field["properties"] = {}
            parents[depth + 1] = field
        else:
            parents.pop(depth + 1, None)
    if route["path"] in ("/api/v5/trade/batch-orders", "/api/v5/trade/cancel-batch-orders", "/api/v5/trade/amend-batch-orders", "/api/v5/trade/cancel-algos", "/api/v5/trade/cancel-advance-algos"):
        schema = {"type": "array", "items": schema}
    route["inputSchema"] = schema
    route["formStatus"] = "documented"
    count += 1
args.catalog.write_text(json.dumps(routes, ensure_ascii=False, indent=2) + "\n")
label_path = args.catalog.parent / "field_labels.json"
label_path.write_text(json.dumps(labels, ensure_ascii=False, indent=2) + "\n")
print(json.dumps({"routes": len(routes), "documentedForms": count, "noParameters": sum(r.get("formStatus") == "no_parameters" for r in routes), "reference": sum(r.get("formStatus") == "reference" for r in routes)}, ensure_ascii=False))
