import ast
from pathlib import Path


def test_package_initializers_only_reexport_public_names() -> None:
    project_root = Path(__file__).resolve().parents[2]
    initializer_paths = sorted((project_root / "src" / "civix").rglob("__init__.py"))
    violations: list[str] = []

    for path in initializer_paths:
        module = ast.parse(path.read_text(), filename=str(path))

        for statement in module.body:
            if _is_allowed_initializer_statement(statement):
                continue

            relative_path = path.relative_to(project_root)
            violations.append(f"{relative_path}:{statement.lineno}")

    assert violations == []


def _is_allowed_initializer_statement(statement: ast.stmt) -> bool:
    if isinstance(statement, ast.Expr):
        return isinstance(statement.value, ast.Constant) and isinstance(
            statement.value.value,
            str,
        )

    if isinstance(statement, ast.Import | ast.ImportFrom):
        return True

    if isinstance(statement, ast.Assign):
        return all(_is_allowed_initializer_target(target) for target in statement.targets)

    return False


def _is_allowed_initializer_target(target: ast.expr) -> bool:
    if not isinstance(target, ast.Name):
        return False

    return target.id in {"__all__", "__version__"}
