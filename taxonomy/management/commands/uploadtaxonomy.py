import csv
import os
from io import StringIO
from collections import defaultdict
from django.core.management.base import BaseCommand
from django.db import connection
from pathlib import Path
from typing import Dict, Tuple, List
from concurrent.futures import ThreadPoolExecutor


class Command(BaseCommand):
    help = "Uploads the taxonomy from NCBI's taxdump"

    def add_arguments(self, parser):
        parser.add_argument(
            "taxdump",
            type=str,
            help="path to the taxdump directory (containing nodes.dmp and names.dmp)",
        )

        parser.add_argument(
            "--table",
            type=str,
            default="taxonomy_taxonomy",
            help="name of the taxonomy table",
        )

    def read_names(self, names_path: Path):
        names: Dict[str, List[str]] = defaultdict(list)

        self.stdout.write(f"Reading names from {names_path}")

        with open(names_path) as names_fh:

            for line in names_fh:
                id, name, _, type, *_ = map(str.strip, line.split("|"))

                if type == "scientific name":
                    names[id].append(name)

        self.stdout.write(f"Read {len(names.keys())} names from {names_path}")

        return names

    def compute_nested_set(self, nodes_path: Path):
        """Pre-compute lft/rgt via DFS traversal."""
        children = defaultdict(list)

        self.stdout.write(f"Computing nested sets from {nodes_path}")

        with open(nodes_path) as f:
            for line in f:
                id, parent, *_ = map(str.strip, line.split("|"))
                if id != "1":
                    children[parent].append(id)

        counter = [1]
        results: Dict[str, Tuple[int, int, int]] = {}

        def dfs(node_id: str, depth: int):
            lft = counter[0]
            counter[0] += 1
            for child in children.get(node_id, []):
                dfs(child, depth + 1)
            rgt = counter[0]
            counter[0] += 1
            results[node_id] = (lft, rgt, depth)

        dfs("1", 0)

        self.stdout.write(f"Computed lft, rgt and depth from {nodes_path}")

        return results

    def handle(self, *args, **options):
        nodes_path = Path(options["taxdump"]) / "nodes.dmp"
        names_path = Path(options["taxdump"]) / "names.dmp"

        with ThreadPoolExecutor() as executor:
            ns_future = executor.submit(self.compute_nested_set, nodes_path)
            names_future = executor.submit(self.read_names, names_path)

            ns_data = ns_future.result()
            names_data = names_future.result()

        with StringIO(newline="") as csv_fh:
            with open(nodes_path) as nodes_fh:
                rows_counter = 0
                writer = csv.writer(csv_fh, delimiter="|")

                self.stdout.write(f"Reading rows from {nodes_path}")

                for line in nodes_fh:
                    id, parent, rank, *_ = map(str.strip, line.split("|"))
                    lft, rgt, depth = ns_data[id]

                    writer.writerow(
                        [
                            id,
                            rank,
                            names_data[id][0] if names_data[id] else "\\N",
                            parent if id != "1" else "\\N",
                            lft,
                            rgt,
                            depth,
                            1,  # tree_id constant
                        ]
                    )
                    rows_counter += 1

                self.stdout.write(f"Read {rows_counter} rows from {nodes_path}")

            csv_fh.seek(0)

            with connection.cursor() as cursor:
                self.stdout.write(
                    f"Truncating table {self.style.SQL_TABLE(options['table'])}"
                )
                cursor.execute(f"TRUNCATE {options['table']} CASCADE;")

                self.stdout.write(
                    f"Inserting rows into {self.style.SQL_TABLE(options['table'])}"
                )
                cursor.copy_from(
                    csv_fh,
                    options["table"],
                    sep="|",
                    columns=("id", "rank", "name", "parent_id", "lft", "rgt", "depth", "tree_id"),
                )

            self.stdout.write(self.style.SUCCESS("Taxonomy loaded successfully!"))
