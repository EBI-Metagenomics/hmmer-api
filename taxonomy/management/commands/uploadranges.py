from io import StringIO
from pathlib import Path
from django.core.management.base import BaseCommand, CommandError
from django.db import connection

REQUIRED_COLUMNS = {"taxonomy_id", "start", "end"}
TABLE_NAME = "taxonomy_range"


class Command(BaseCommand):
    help = "Upload sequence ranges for taxonomies. Note: file name has to correspond to the database"

    def add_arguments(self, parser):
        parser.add_argument("ranges", type=str, help="path to the tsv containing range and tax info")

    def handle(self, *args, **options):
        database = Path(options["ranges"]).stem

        with connection.cursor() as cursor, open(options["ranges"]) as tsv_fh:
            header_line = tsv_fh.readline()
            columns = list(map(str.strip, header_line.split("\t")))
            columns_set = set(columns)

            if not REQUIRED_COLUMNS.issubset(columns_set):
                missing = REQUIRED_COLUMNS - columns_set
                raise CommandError(
                    f"TSV file {options["ranges"]} is missing required columns: {', '.join(missing)}"
                )

            if difference := columns_set.difference(REQUIRED_COLUMNS):
                raise CommandError(
                    f"TSV file {options["ranges"]} contains unknown columns: {', '.join(difference)}"
                )

            self.stdout.write(self.style.WARNING(f"Deleting existing range records for database {database}..."))
            self.stdout.flush()
            cursor.execute(f'DELETE FROM {TABLE_NAME} WHERE "database" = %s', (database,))

            self.stdout.write(f"Inserting rows into {self.style.SQL_TABLE(TABLE_NAME)}")
            self.stdout.flush()

            modified_fh = StringIO(''.join(line.rstrip("\n") + f"\t{database}\n" for line in tsv_fh))

            cursor.copy_from(modified_fh, TABLE_NAME, sep="\t", null="", columns=columns + ["database"])

            self.stdout.write(self.style.SUCCESS("Taxonomy ranges loaded successfully!"))
            self.stdout.flush()
