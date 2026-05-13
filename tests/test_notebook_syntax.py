"""
Validates all Databricks notebooks parse as valid Python.

Strips Databricks notebook directives (# MAGIC, # COMMAND, # DBTITLE)
before parsing with the built-in AST module.
"""

import ast
import glob
import pytest


def notebook_python_lines(path):
    """Extract Python code lines from a Databricks notebook .py file."""
    with open(path) as f:
        lines = f.readlines()

    return [
        line for line in lines
        if not line.startswith("# MAGIC")
        and not line.startswith("# DBTITLE")
        and line.strip() != "# COMMAND ----------"
        and not line.startswith("# Databricks notebook")
    ]


NBS = sorted(glob.glob("databricks/notebooks/*.py"))


@pytest.mark.parametrize("path", NBS)
def test_notebook_syntax(path):
    """Every notebook must parse as valid Python after stripping directives."""
    python_code = "".join(notebook_python_lines(path))
    try:
        ast.parse(python_code)
    except SyntaxError as e:
        pytest.fail(f"{path} has a syntax error: {e}")


@pytest.mark.parametrize("path", NBS)
def test_notebook_not_empty(path):
    """Every notebook must contain at least one meaningful Python statement."""
    python_code = "".join(notebook_python_lines(path))
    tree = ast.parse(python_code)
    # Count top-level statements (imports, function defs, expressions, etc.)
    stmt_count = sum(
        1 for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.ClassDef, ast.Import, ast.ImportFrom,
                             ast.Expr, ast.Assign, ast.AugAssign, ast.AnnAssign, ast.For,
                             ast.While, ast.If, ast.With, ast.Try, ast.Return))
    )
    assert stmt_count > 0, f"{path} contains no Python statements"
