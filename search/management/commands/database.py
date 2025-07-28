from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from search.models import Database


class Command(BaseCommand):
    help = "Enables, disables or pauses searches for a database"

    def add_arguments(self, parser):
        parser.add_argument("database", type=str)
        parser.add_argument("status", type=str, choices=["enable", "disable", "pause"])

    def handle(self, *args, **options):
        if options["database"] not in settings.HMMER.databases:
            raise CommandError(f"Database '{options['database']}' is not supported")

        try:
            database = Database.objects.get(id=options["database"])
        except Database.DoesNotExist:
            raise CommandError(f"Entry for database '{options['database']}' not found")

        if options["status"] == "enable":
            database.status = Database.StatusChoices.ENABLED
        elif options["status"] == "disable":
            database.status = Database.StatusChoices.DISABLED
        elif options["status"] == "pause":
            database.status = Database.StatusChoices.PAUSED

        database.save()

        self.stdout.write(f"Searches for database '{database.id}' are {database.status}")
