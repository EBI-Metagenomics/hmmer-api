from datetime import timedelta
from django.core.management.base import BaseCommand
from django.utils import timezone
from search.models import HmmerJob


class Command(BaseCommand):
    help = "Deletes jobs as well as data associated with the job older than specified days (default is 30 days)"

    def add_arguments(self, parser):
        parser.add_argument("--days", type=int, default=30, help="jobs age")
        parser.add_argument("--dry-run", action="store_true", help="perform a dry run")
        parser.add_argument("--list", action="store_true", help="list fetched jobs")

    def handle(self, *args, **options):
        point_in_time = timezone.now() - timedelta(days=options["days"])

        self.stdout.write(f"Fetching jobs submitted before {point_in_time}")
        jobs_to_delete = HmmerJob.objects.filter(date_submitted__lte=point_in_time, parent=None)

        if options["dry_run"]:
            num_of_deleted = 0

            for job in jobs_to_delete:
                num_of_deleted += 1 + job.get_descendant_count()

            self.stdout.write(f"Would have deleted {num_of_deleted} jobs")
        else:
            num_of_deleted, _ = jobs_to_delete.delete()
            self.stdout.write(f"Deleted {num_of_deleted} jobs")

        if options["list"]:
            for job in jobs_to_delete:
                self.stdout.write(f"{job.algo} {job.id}")
                descendants = job.get_descendants()

                for descendant in descendants:
                    self.stdout.write(f"|--{descendant.algo} {descendant.id}")
