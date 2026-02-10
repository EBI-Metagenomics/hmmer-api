import itertools
import math
from typing import List, Optional

import numpy as np
import portion as P
from django.contrib.postgres.indexes import GinIndex
from django.contrib.postgres.search import SearchVector, SearchVectorField
from django.db import models
from pydantic import BaseModel
from result.models import P7Hit, Result
from treebeard.ns_tree import NS_Node


# Setup intervals
class IntInterval(P.AbstractDiscreteInterval):
    _step = 1


P.create_api(IntInterval)


def format_evalue(value: float):
    if value < 0.0001:
        return f"{value:.1e}"
    else:
        return f"{value:.6g}"


class Taxonomy(NS_Node):
    id = models.PositiveIntegerField(unique=True, primary_key=True)
    parent = models.ForeignKey(
        "self",
        related_name="children_set",
        on_delete=models.CASCADE,
        null=True,
        db_index=True,
    )
    name = models.CharField(max_length=255)
    rank = models.CharField(max_length=255)
    search = models.GeneratedField(
        db_persist=True,
        expression=SearchVector("name", config="simple"),
        output_field=SearchVectorField(),
    )

    node_order_by = ["id"]

    class Meta(NS_Node.Meta):
        indexes = [GinIndex(fields=["search"])]

    @classmethod
    def dump(cls):
        return []


class Range(models.Model):
    pk = models.CompositePrimaryKey("taxonomy", "database")
    database = models.CharField(max_length=32)
    taxonomy = models.ForeignKey(Taxonomy, on_delete=models.CASCADE)
    start = models.IntegerField(null=True, blank=True)
    end = models.IntegerField(null=True, blank=True)

    @classmethod
    def get_seqdb_ranges_from_taxonomy(
        cls, database: str, include: List[int], exclude: List[int]
    ):
        if not include and not exclude:
            return ""

        if include:
            intervals = IntInterval(
                *(
                    P.closed(range.start, range.end, klass=IntInterval)
                    for range in cls.objects.filter(
                        database=database, taxonomy__id__in=include
                    )
                )
            )
        else:
            range_for_root = cls.objects.get(database=database, taxonomy__id=1)
            intervals = P.closed(
                range_for_root.start, range_for_root.end, klass=IntInterval
            )

        if exclude:
            intervals = intervals - IntInterval(
                *(
                    P.closed(range.start, range.end, klass=IntInterval)
                    for range in cls.objects.filter(
                        database=database, taxonomy__id__in=exclude
                    )
                )
            )

        return f"--seqdb_ranges {P.to_string(intervals, sep='..', disj=',', left_closed='', right_closed='', left_open='', right_open='')}"

    class Meta:
        indexes = [models.Index(fields=["taxonomy", "database"])]


class TaxonomyResult(BaseModel):
    taxonomy_id: int | None
    species: str | None
    count: int

    @classmethod
    def from_result(cls, result: Result):
        taxonomy_aggregation: List[TaxonomyResult] = []

        for taxonomy_id, group in itertools.groupby(
            sorted(result.hits, key=lambda hit: hit.metadata.taxonomy_id or -1),
            key=lambda hit: hit.metadata.taxonomy_id,
        ):
            hits = list(group)
            taxonomy_aggregation.append(
                TaxonomyResult(
                    taxonomy_id=taxonomy_id,
                    species=hits[0].metadata.species or "Unknown",
                    count=len(hits),
                )
            )

        return sorted(taxonomy_aggregation, key=lambda row: row.count, reverse=True)


class TaxonomyTree(BaseModel):
    id: int
    name: str
    hitcount: Optional[int]
    hitdist: Optional[List[int]]
    children: "Optional[List[TaxonomyTree]]"

    @classmethod
    def from_result(cls, result: Result):
        included_hits = [hit for hit in result.hits if hit.is_included]

        histogram, _ = np.histogram(
            [
                -math.log(hit.evalue) if hit.evalue > 0 else 1000
                for hit in included_hits
            ],
            bins=30,
        )
        sorted_hits = sorted(
            included_hits, key=lambda hit: hit.metadata.lineage[0] or np.inf
        )
        grouped_hits = itertools.groupby(
            sorted_hits, key=lambda hit: hit.metadata.lineage[0] or np.inf
        )
        children = [TaxonomyTree.build_tree(list(group)) for _, group in grouped_hits]

        return TaxonomyTree(
            id=1,
            name="All",
            hitdist=histogram.tolist(),
            hitcount=len(included_hits),
            children=children,
        )

    @classmethod
    def build_tree(cls, hits: List[P7Hit], depth=0):
        if depth < len(hits[0].metadata.lineage) - 1:
            if hits[0].metadata.lineage[depth] is None:
                return TaxonomyTree.build_tree(hits, depth=depth + 1)

            histogram, _ = np.histogram(
                [-math.log(hit.evalue) if hit.evalue > 0 else 1000 for hit in hits],
                bins=30,
            )
            id = hits[0].metadata.lineage[depth]
            taxonomy = Taxonomy.objects.get(id=id)
            sorted_hits = sorted(
                hits, key=lambda hit: hit.metadata.lineage[depth + 1] or np.inf
            )
            grouped_hits = itertools.groupby(
                sorted_hits, key=lambda hit: hit.metadata.lineage[depth + 1] or np.inf
            )
            children = filter(
                None,
                [
                    TaxonomyTree.build_tree(list(group), depth=depth + 1)
                    for _, group in grouped_hits
                ],
            )

            return TaxonomyTree(
                id=id,
                name=taxonomy.name,
                hitdist=histogram.tolist(),
                hitcount=len(hits),
                children=children,
            )
        else:
            if hits[0].metadata.lineage[depth] is None:
                return None

            histogram, _ = np.histogram(
                [-math.log(hit.evalue) if hit.evalue > 0 else 1000 for hit in hits],
                bins=30,
            )
            id = hits[0].metadata.lineage[depth]
            taxonomy = Taxonomy.objects.get(id=id)

            return TaxonomyTree(
                id=id,
                name=taxonomy.name,
                hitdist=histogram.tolist(),
                hitcount=len(hits),
                children=None,
            )


class TaxonomyDistributionGraph(BaseModel):
    data: List[List[int]]
    labels: List[str]
    categories: List[str]
    colors: List[str]

    @classmethod
    def from_result(cls, result: Result) -> "TaxonomyDistributionGraph":
        number_of_bins = 30

        color_map = {
            "bacteria": "#b65417",
            "archaea": "#3b6fb6",
            "eukaryota": "#f4c61f",
            "viruses": "#d41645",
            "unclassified sequences": "#a9abaa",
            "other sequences": "#373a36",
        }

        # TODO fix taxonomy according to ncbi
        # 2,Bacteria
        # 2157,Archaea
        # 2759,Eukaryota
        # 10239,Viruses
        # 12908,unclassified sequences
        # 28384,other sequences
        # 131567,cellular organisms
        # 2787823,unclassified entries
        # 2787854,other entries

        superkingdoms_map = {
            name.lower(): id
            for name, id in Taxonomy.objects.filter(
                id__in=[2, 2157, 2759, 10239, 12908, 28384]
            ).values_list("name", "id")
        }

        taxonomy_id_lookup = {
            taxonomy_id: taxonomy_id for taxonomy_id in superkingdoms_map.values()
        }

        if "unclassified entries" in superkingdoms_map:
            taxonomy_id_lookup[superkingdoms_map["unclassified entries"]] = (
                superkingdoms_map["unclassified sequences"]
            )

        if "other entries" in superkingdoms_map:
            taxonomy_id_lookup[superkingdoms_map["other entries"]] = superkingdoms_map[
                "other sequences"
            ]

        taxonomy_id_lookup[2787823] = 12908
        taxonomy_id_lookup[2787854] = 28384
        taxonomy_id_lookup[None] = superkingdoms_map["unclassified sequences"]

        values = [
            -math.log(hit.evalue) if hit.evalue > 0 else 1000
            for hit in result.hits
            if hit.is_included
        ]
        superkingdoms = [
            taxonomy_id_lookup[hit.metadata.lineage[0]]
            for hit in result.hits
            if hit.is_included
        ]

        bins_y = np.unique(list(taxonomy_id_lookup.values()))
        bins_y = np.append(bins_y, bins_y[-1] + 1)
        histogram, x_edges, _ = np.histogram2d(
            values, superkingdoms, bins=[number_of_bins, bins_y]
        )

        return {
            "data": histogram.tolist(),
            "labels": [
                f"{format_evalue(math.exp(-x_edges[i]))} ≤ e-value < {format_evalue(math.exp(-x_edges[i + 1]))}"
                for i in range(number_of_bins)
            ],
            "categories": map(str.title, color_map.keys()),
            "colors": list(color_map.values()),
        }
