import csv
import os
from io import StringIO
from collections import defaultdict
from django.core.management.base import BaseCommand
from django.db import connection


class Command(BaseCommand):
    help = "Uploads the taxonomy from NCBI's taxdump"

    def add_arguments(self, parser):
        parser.add_argument(
            "taxdump", type=str, help="path to the taxdump directory (containing nodes.dmp and names.dmp)"
        )

        parser.add_argument("--table", type=str, default="taxonomy_taxonomy", help="name of the taxonomy table")

    def handle(self, *args, **options):
        nodes_path = os.path.join(options["taxdump"], "nodes.dmp")
        names_path = os.path.join(options["taxdump"], "names.dmp")

        names = defaultdict(list)

        with open(names_path) as names_fh:
            self.stdout.write(f"Reading names from {names_path}")

            for line in names_fh:
                id, name, _, type, *_ = map(str.strip, line.split("|"))

                if type == "scientific name":
                    names[id].append(name)

            self.stdout.write(f"Read {len(names.keys())} names from {names_path}")

        with StringIO(newline="") as csv_fh:
            with open(nodes_path) as nodes_fh:
                rows_counter = 0
                writer = csv.writer(csv_fh, delimiter="|")

                self.stdout.write(f"Reading rows from {nodes_path}")

                for line in nodes_fh:
                    id, parent, rank, *_ = map(str.strip, line.split("|"))
                    writer.writerow([id, rank, names[id][0] if names[id] else "\\N", parent if id != "1" else "\\N"])
                    rows_counter += 1

                self.stdout.write(f"Read {rows_counter} rows from {nodes_path}")

            csv_fh.seek(0)

            with connection.cursor() as cursor:
                self.stdout.write(f"Truncating table {self.style.SQL_TABLE(options["table"])}")
                cursor.execute(f"TRUNCATE {options["table"]} CASCADE;")

                self.stdout.write(f"Inserting rows into {self.style.SQL_TABLE(options["table"])}")
                cursor.copy_from(csv_fh, options["table"], sep="|", columns=("id", "rank", "name", "parent_id"))

            self.stdout.write(self.style.SUCCESS("Taxonomy loaded successfully!"))
