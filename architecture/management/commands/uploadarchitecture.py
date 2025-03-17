import csv
import sys
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from architecture.models import Architecture

ARCHITECTURE_REQUIRED_COLUMNS = {"sequence_index", "database", "accessions", "names", "score", "graphics"}

csv.field_size_limit(sys.maxsize)


class Command(BaseCommand):
    help = "Uploads the architecture from a CSV file"

    def add_arguments(self, parser):
        parser.add_argument("architecture_file", type=str, help="the CSV file containing the architecture")

        parser.add_argument("--replace", action="store_true", help="replace existing records")

    def handle(self, *args, **options):
        architecture_file = options["architecture_file"]

        with open(architecture_file, newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            header_set = set(reader.fieldnames)

            if not ARCHITECTURE_REQUIRED_COLUMNS.issubset(header_set):
                missing = ARCHITECTURE_REQUIRED_COLUMNS - header_set
                raise CommandError(f"CSV file {architecture_file} is missing required columns: {', '.join(missing)}")

            with transaction.atomic():
                if options["replace"]:
                    self.stdout.write(self.style.WARNING("Deleting existing architecture records..."))
                    Architecture.objects.all().delete()

                self.stdout.write("Loading architecture records...")

                for row in reader:
                    Architecture.objects.create(**row)

            self.stdout.write(self.style.SUCCESS("Architecture records loaded successfully!"))
