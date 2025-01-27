import csv
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from taxonomy.models import Taxonomy, Range

TAXONOMY_REQUIRED_COLUMNS = {"taxonomy_id", "parent_id", "name", "rank"}
RANGE_REQUIRED_COLUMNS = {"taxonomy_id", "database", "start", "end"}


class Command(BaseCommand):
    help = "Uploads the taxonomy from a CSV file"

    def add_arguments(self, parser):
        parser.add_argument("taxonomy_file", type=str, help="the CSV file containing the taxonomy")
        parser.add_argument("range_file", nargs="*", type=str, help="the CSV file containing the database ranges")

        parser.add_argument("--replace", action="store_true", help="replace existing records (applies to ranges only)")

    def handle(self, *args, **options):
        taxonomy_file = options["taxonomy_file"]
        range_files = options["range_file"]

        # with open(taxonomy_file, newline="") as csvfile:
        #     reader = csv.DictReader(csvfile)
        #     header_set = set(reader.fieldnames)

        #     if not TAXONOMY_REQUIRED_COLUMNS.issubset(header_set):
        #         missing = TAXONOMY_REQUIRED_COLUMNS - header_set
        #         raise CommandError(f"CSV file {taxonomy_file} is missing required columns: {', '.join(missing)}")

        #     rows = list(reader)

        #     self.stdout.write(f"Read {len(rows)} rows from {taxonomy_file}")
        #     nodes = {}

        #     for row in rows:
        #         taxonomy_id = int(row["taxonomy_id"])
        #         nodes[taxonomy_id] = {
        #             "data": {"taxonomy_id": taxonomy_id, "name": row["name"], "rank": row["rank"]},
        #             "children": [],
        #         }

        #     for row in rows:
        #         taxonomy_id = int(row["taxonomy_id"])
        #         parent_id = int(row["parent_id"])

        #         if taxonomy_id != parent_id and parent_id in nodes:
        #             nodes[parent_id]["children"].append(nodes[taxonomy_id])

        #     root = nodes[1]

        #     with transaction.atomic():
        #         self.stdout.write("Deleting existing taxonomy records...")
        #         Taxonomy.objects.all().delete()

        #         self.stdout.write("Loading taxonomy records...")
        #         Taxonomy.load_bulk([root])

        #     self.stdout.write(self.style.SUCCESS("Taxonomy records loaded successfully!"))

        if not range_files:
            self.stdout.write("No range files provided, skipping range records...")
            return

        ranges = []

        for range_file in range_files:
            with open(range_file, newline="") as csvfile:
                reader = csv.DictReader(csvfile)
                header_set = set(reader.fieldnames)

                if not RANGE_REQUIRED_COLUMNS.issubset(header_set):
                    missing = RANGE_REQUIRED_COLUMNS - header_set
                    raise CommandError(f"CSV file {range_file} is missing required columns: {', '.join(missing)}")

                range_objects = [Range(**row) for row in reader if row["start"] and row["end"]]
                ranges.extend(range_objects)

            self.stdout.write(f"Read {len(range_objects)} rows from {range_file}")

        with transaction.atomic():
            if options["replace"]:
                self.stdout.write(self.style.WARNING("Deleting existing range records..."))
                Range.objects.all().delete()

            self.stdout.write("Loading range records...")
            Range.objects.bulk_create(ranges)

        self.stdout.write(self.style.SUCCESS("Range records loaded successfully!"))
