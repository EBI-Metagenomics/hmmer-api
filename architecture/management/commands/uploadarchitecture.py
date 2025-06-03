from django.core.management.base import BaseCommand, CommandError
from django.db import connection

ARCHITECTURE_REQUIRED_COLUMNS = {"sequence_index", "accessions", "names", "score", "graphics", "database"}


class Command(BaseCommand):
    help = "Uploads the architecture from a CSV file"

    def add_arguments(self, parser):
        parser.add_argument("architecture_file", type=str, help="TSV file containing the architecture")

        parser.add_argument(
            "--replace", action="store_true", help="replace existing records - THIS OPERATION IS DESTRUCTIVE"
        )
        parser.add_argument(
            "--table", type=str, default="architecture_architecture", help="name of the architecture table"
        )

    def handle(self, *args, **options):
        with connection.cursor() as cursor, open(options["architecture_file"]) as tsv_fh:
            if options["replace"]:
                self.stdout.write(self.style.WARNING("Deleting existing architecture records..."))
                self.stdout.flush()
                cursor.execute(f"TRUNCATE {options["table"]} CASCADE;")

            self.stdout.write(f"Inserting rows into {self.style.SQL_TABLE(options["table"])}")
            self.stdout.flush()

            header_line = tsv_fh.readline()
            columns = list(map(str.strip, header_line.split("\t")))
            columns_set = set(columns)

            if not ARCHITECTURE_REQUIRED_COLUMNS.issubset(columns_set):
                missing = ARCHITECTURE_REQUIRED_COLUMNS - columns_set
                raise CommandError(
                    f"TSV file {options["architecture_file"]} is missing required columns: {', '.join(missing)}"
                )

            if difference := columns_set.difference(ARCHITECTURE_REQUIRED_COLUMNS):
                raise CommandError(
                    f"TSV file {options["architecture_file"]} contains unknown columns: {', '.join(difference)}"
                )

            cursor.copy_from(tsv_fh, options["table"], sep="\t", columns=columns)

            self.stdout.write(self.style.SUCCESS("Architecture loaded successfully!"))
            self.stdout.flush()
