from __future__ import annotations

import hashlib
import json
from pathlib import Path, PurePosixPath
from typing import Any

from git import InvalidGitRepositoryError, NoSuchPathError, Repo

_MAX_TREE_ENTRIES = 2_000
_MAX_DOCUMENT_BYTES = 131_072
_MAX_SINGLE_DOCUMENT_BYTES = 32_768
_MAX_USER_REQUEST_BYTES = 100_000
_DOCUMENT_NAMES = {
    "readme.md",
    "goal.md",
    "architecture.md",
    "arch_plan.md",
    "pyproject.toml",
    "package.json",
    "cargo.toml",
    "go.mod",
    "requirements.txt",
    "docker-compose.yml",
    "docker-compose.yaml",
}
_DOCUMENT_SUFFIXES = {".md", ".rst"}


class PlanningContextError(RuntimeError):
    """The repository could not be represented by a trustworthy bounded snapshot."""


def _relevant_document(path: str) -> bool:
    item = Path(path)
    lowered = item.name.lower()
    return bool(
        lowered in _DOCUMENT_NAMES
        or (
            item.parts
            and item.parts[0].lower() in {"docs", ".github"}
            and item.suffix.lower() in _DOCUMENT_SUFFIXES
        )
    )


def build_planning_context(source_repository: Path | None, user_request: str) -> dict[str, Any]:
    """Capture a deterministic, bounded repository snapshot before planning.

    Only Git-tracked paths are exposed to the planning provider. Binary/unreadable files are
    represented by path, while bounded delivery docs and manifests include their text.
    """

    if not user_request.strip():
        raise PlanningContextError("planning requires a nonempty user request")
    if len(user_request.encode("utf-8")) > _MAX_USER_REQUEST_BYTES:
        raise PlanningContextError(
            f"user request exceeds the {_MAX_USER_REQUEST_BYTES}-byte planning limit"
        )
    if source_repository is None:
        tree: list[str] = []
        documents: dict[str, str] = {}
        source_revision = "EMPTY_WORKSPACE"
        repository = None
    else:
        root = source_repository.resolve()
        try:
            repository = Repo(root)
        except (InvalidGitRepositoryError, NoSuchPathError) as error:
            raise PlanningContextError(
                "source repository must be a readable Git checkout"
            ) from error
        if repository.bare:
            raise PlanningContextError("source repository cannot be bare")
        if repository.is_dirty(untracked_files=True):
            raise PlanningContextError(
                "source repository has uncommitted changes; planning requires an immutable revision"
            )
        try:
            source_revision = repository.head.commit.hexsha
            tracked = repository.git.ls_files("-z").split("\x00")
        except Exception as error:  # GitPython maps command failures to several subclasses.
            raise PlanningContextError(
                "source repository HEAD and tracked files are required"
            ) from error
        tree = sorted(item.replace("\\", "/") for item in tracked if item)
        if len(tree) > _MAX_TREE_ENTRIES:
            raise PlanningContextError(
                f"repository has {len(tree)} tracked paths; limit is {_MAX_TREE_ENTRIES}"
            )
        documents = {}
        used_bytes = 0
        for relative in tree:
            relative_path = PurePosixPath(relative)
            if relative_path.is_absolute() or ".." in relative_path.parts:
                raise PlanningContextError("Git returned an unsafe tracked path")
            if not _relevant_document(relative):
                continue
            path = root / Path(relative)
            # A tracked documentation symlink could otherwise disclose a file outside the
            # repository to the planning provider. Keep the path in the manifest, but never
            # follow it when assembling document text.
            if path.is_symlink():
                continue
            try:
                raw = path.read_bytes()
                if len(raw) > _MAX_SINGLE_DOCUMENT_BYTES or b"\x00" in raw:
                    continue
                text = raw.decode("utf-8")
            except (OSError, UnicodeDecodeError):
                continue
            size = len(text.encode("utf-8"))
            if used_bytes + size > _MAX_DOCUMENT_BYTES:
                break
            documents[relative] = text
            used_bytes += size

    canonical = {
        "user_request": user_request,
        "source_revision": source_revision,
        "tracked_tree": tree,
        "documents": documents,
    }
    context_hash = hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return {**canonical, "planning_context_hash": context_hash}
