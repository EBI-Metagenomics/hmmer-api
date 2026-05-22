from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from django.core.management import CommandError, call_command
from psycopg2 import sql

from taxonomy.management.commands import uploadranges, uploadtaxonomy


def _write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def _build_cursor_context():
    cursor = MagicMock()
    context_manager = MagicMock()
    context_manager.__enter__.return_value = cursor
    context_manager.__exit__.return_value = False
    return cursor, context_manager


def test_uploadtaxonomy_uses_safe_composed_sql_and_fixed_table(tmp_path):
    taxdump = tmp_path / "taxdump"
    taxdump.mkdir()
    _write_text(taxdump / "nodes.dmp", "1 | 1 | no rank |\n")
    _write_text(taxdump / "names.dmp", "1 | root | | scientific name |\n")

    cursor, cursor_context = _build_cursor_context()

    with patch.object(uploadtaxonomy.connection, "cursor", return_value=cursor_context):
        call_command("uploadtaxonomy", str(taxdump))

    execute_args = cursor.execute.call_args.args
    assert isinstance(execute_args[0], sql.Composable)
    assert cursor.copy_from.call_args.args[1] == uploadtaxonomy.TABLE_NAME


def test_uploadranges_uses_safe_composed_sql_and_bound_database_value(tmp_path):
    ranges_file = tmp_path / "pdb.tsv"
    _write_text(ranges_file, "taxonomy_id\tstart\tend\n1\t10\t20\n")

    cursor, cursor_context = _build_cursor_context()

    with patch.object(uploadranges.connection, "cursor", return_value=cursor_context):
        call_command("uploadranges", str(ranges_file))

    execute_args = cursor.execute.call_args.args
    assert isinstance(execute_args[0], sql.Composable)
    assert execute_args[1] == ("pdb",)
    assert cursor.copy_from.call_args.args[1] == uploadranges.TABLE_NAME


def test_uploadranges_rejects_unknown_columns(tmp_path):
    ranges_file = tmp_path / "pdb.tsv"
    _write_text(ranges_file, "taxonomy_id\tstart\tend\textra\n1\t10\t20\tbad\n")

    cursor, cursor_context = _build_cursor_context()

    with patch.object(uploadranges.connection, "cursor", return_value=cursor_context):
        with pytest.raises(CommandError, match="contains unknown columns"):
            call_command("uploadranges", str(ranges_file))
