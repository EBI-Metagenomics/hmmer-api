from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from search.models import Database


class Command(BaseCommand):
    help = "Enables, disables or pauses searches for a database"

    def add_arguments(self, parser):
        parser.add_argument("status", type=str, choices=["enable", "disable", "pause"])
        parser.add_argument("database", type=str, nargs="+")

    def handle(self, *args, **options):
        for database in options["database"]:
            if database not in settings.HMMER.databases:
                raise CommandError(f"Database '{database}' is not supported")

        databases = Database.objects.filter(id__in=options["database"])

        missing_databases = set(options["database"]) - set([database.id for database in databases])

        if missing_databases:
            raise CommandError(f"Entries for databases {", ".join(missing_databases)} not found")

        if options["status"] == "enable":
            status_to_set = Database.StatusChoices.ENABLED
        elif options["status"] == "disable":
            status_to_set = Database.StatusChoices.DISABLED
        elif options["status"] == "pause":
            status_to_set = Database.StatusChoices.PAUSED
        else:
            status_to_set = Database.StatusChoices.ENABLED

        for database in databases:
            database.status = status_to_set

        Database.objects.bulk_update(databases, ["status"])

        self.stdout.write(f"Searches for databases {", ".join(options["database"])} are {status_to_set.value}")
