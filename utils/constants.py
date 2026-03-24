from pathlib import Path


def project_root(anchor: str = "pyproject.toml"):
    path = Path.cwd()
    for parent in [path] + list(path.parents):
        if (parent / anchor).exists():
            return parent
    raise FileNotFoundError(
        f"Could not find {anchor} in the parent directories of {path}"
    )


ROOTDIR = project_root()
DATADIR = ROOTDIR / "data"
FIGURESDIR = ROOTDIR / "figures"
MIN_VALUE_THRESHOLD: float = 1e-16
