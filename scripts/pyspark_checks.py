#!/usr/bin/env python3
"""PySpark-specific checks that SonarQube has no rules for.

SonarQube understands Python but knows nothing about Spark. This script walks
the AST looking for the patterns that actually hurt on a Databricks cluster,
then writes the findings in SonarQube's *generic issue import* format so they
show up on the same dashboard as the built-in rules.

Usage:
    python scripts/pyspark_checks.py src notebooks > pyspark-issues.json

Then in sonar-project.properties:
    sonar.externalIssuesReportPaths=pyspark-issues.json
"""

import ast
import json
import sys
from pathlib import Path

# rule_id -> (severity, message)
RULES = {
    "pyspark:S001": ("BLOCKER", "collect()/toPandas() pulls the whole dataset to the driver - use limit() or write to a table"),
    "pyspark:S002": ("MAJOR", "inferSchema=True is non-deterministic and slow - declare an explicit schema"),
    "pyspark:S003": ("MAJOR", "display() is a notebook-only helper - remove it from production code"),
    "pyspark:S004": ("CRITICAL", "hardcoded /mnt/ path - use a Unity Catalog volume or an external location"),
    "pyspark:S005": ("MAJOR", "count() called inside a loop or repeatedly - it triggers a full scan each time"),
}

DRIVER_PULLS = {"collect", "toPandas"}


class SparkVisitor(ast.NodeVisitor):
    def __init__(self, path):
        self.path = path
        self.issues = []
        self.count_calls = 0

    def _add(self, rule_id, node):
        severity, message = RULES[rule_id]
        self.issues.append(
            {
                "engineId": "pyspark-checks",
                "ruleId": rule_id,
                "effortMinutes": 15,
                "severity": severity,
                "type": "CODE_SMELL" if severity == "MAJOR" else "BUG",
                "primaryLocation": {
                    "message": message,
                    "filePath": self.path,
                    "textRange": {"startLine": node.lineno},
                },
            }
        )

    def visit_Call(self, node):
        func = node.func
        if isinstance(func, ast.Attribute):
            if func.attr in DRIVER_PULLS:
                self._add("pyspark:S001", node)
            if func.attr == "count":
                self.count_calls += 1
                if self.count_calls > 2:
                    self._add("pyspark:S005", node)
        if isinstance(func, ast.Name) and func.id == "display":
            self._add("pyspark:S003", node)

        for kw in node.keywords:
            if kw.arg == "inferSchema" and isinstance(kw.value, ast.Constant) and kw.value.value is True:
                self._add("pyspark:S002", node)

        self.generic_visit(node)

    def visit_Constant(self, node):
        if isinstance(node.value, str) and node.value.startswith("/mnt/"):
            self._add("pyspark:S004", node)
        self.generic_visit(node)


def scan(path: Path):
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError:
        return []
    visitor = SparkVisitor(str(path))
    visitor.visit(tree)
    return visitor.issues


def main(argv):
    roots = argv[1:] or ["src", "notebooks"]
    issues = []
    for root in roots:
        for py in sorted(Path(root).rglob("*.py")):
            issues.extend(scan(py))
    json.dump({"issues": issues}, sys.stdout, indent=2)
    sys.stdout.write("\n")
    print(f"{len(issues)} PySpark issue(s) found", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
