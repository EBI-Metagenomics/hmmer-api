import itertools
import math
import numpy as np
from typing import List, Optional
from django.db import models
from django.contrib.postgres.search import SearchVector, SearchVectorField
from treebeard.al_tree import AL_Node
from pydantic import BaseModel
from result.models import Result, P7Hit


def format_evalue(value: float):
    if value < 0.0001:
        return f"{value:.1e}"
    else:
        return f"{value:.6g}"


class Taxonomy(AL_Node):
    id = models.PositiveIntegerField(unique=True, primary_key=True)
    parent = models.ForeignKey("self", related_name="children_set", on_delete=models.CASCADE, null=True, db_index=True)
    name = models.CharField(max_length=255)
    rank = models.CharField(max_length=255)
    search = models.GeneratedField(
        db_persist=True, expression=SearchVector("name", config="simple"), output_field=SearchVectorField()
    )

    @classmethod
    def dump(cls):
        return []


class Range(models.Model):
    database = models.CharField(max_length=32)
    taxonomy = models.ForeignKey(Taxonomy, on_delete=models.SET_NULL, null=True, blank=True)
    start = models.IntegerField(null=True, blank=True)
    end = models.IntegerField(null=True, blank=True)


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
                TaxonomyResult(taxonomy_id=taxonomy_id, species=hits[0].metadata.species or "Unknown", count=len(hits))
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
        histogram, _ = np.histogram([-math.log(hit.evalue) for hit in result.hits], bins=30)
        sorted_hits = sorted(result.hits, key=lambda hit: hit.metadata.lineage[0] or np.inf)
        grouped_hits = itertools.groupby(sorted_hits, key=lambda hit: hit.metadata.lineage[0] or np.inf)
        children = [TaxonomyTree.build_tree(list(group)) for _, group in grouped_hits]

        return TaxonomyTree(id=1, name="All", hitdist=histogram.tolist(), hitcount=len(result.hits), children=children)

    @classmethod
    def build_tree(cls, hits: List[P7Hit], depth=0):
        if depth < len(hits[0].metadata.lineage) - 1:
            if hits[0].metadata.lineage[depth] is None:
                return TaxonomyTree.build_tree(hits, depth=depth + 1)

            histogram, _ = np.histogram([-math.log(hit.evalue) for hit in hits], bins=30)
            id = hits[0].metadata.lineage[depth]
            taxonomy = Taxonomy.objects.get(id=id)
            sorted_hits = sorted(hits, key=lambda hit: hit.metadata.lineage[depth + 1] or np.inf)
            grouped_hits = itertools.groupby(sorted_hits, key=lambda hit: hit.metadata.lineage[depth + 1] or np.inf)
            children = filter(
                None, [TaxonomyTree.build_tree(list(group), depth=depth + 1) for _, group in grouped_hits]
            )

            return TaxonomyTree(
                id=id, name=taxonomy.name, hitdist=histogram.tolist(), hitcount=len(hits), children=children
            )
        else:
            if hits[0].metadata.lineage[depth] is None:
                return None

            histogram, _ = np.histogram([-math.log(hit.evalue) for hit in hits], bins=30)
            id = hits[0].metadata.lineage[depth]
            taxonomy = Taxonomy.objects.get(id=id)

            return TaxonomyTree(
                id=id, name=taxonomy.name, hitdist=histogram.tolist(), hitcount=len(hits), children=None
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

        superkingdoms_map = {
            name.lower(): id
            for name, id in Taxonomy.objects.filter(rank="superkingdom").values_list("name", "taxonomy_id")
        }

        taxonomy_id_lookup = {taxonomy_id: taxonomy_id for taxonomy_id in superkingdoms_map.values()}

        if "unclassified entries" in superkingdoms_map:
            taxonomy_id_lookup[superkingdoms_map["unclassified entries"]] = superkingdoms_map["unclassified sequences"]

        if "other entries" in superkingdoms_map:
            taxonomy_id_lookup[superkingdoms_map["other entries"]] = superkingdoms_map["other sequences"]

        taxonomy_id_lookup[None] = superkingdoms_map["unclassified sequences"]

        values = [-math.log(hit.evalue) if hit.evalue > 0 else 1000 for hit in result.hits if hit.is_included]
        superkingdoms = [taxonomy_id_lookup[hit.metadata.lineage[0]] for hit in result.hits if hit.is_included]

        bins_y = np.unique(list(taxonomy_id_lookup.values()))
        bins_y = np.append(bins_y, bins_y[-1] + 1)
        histogram, x_edges, _ = np.histogram2d(values, superkingdoms, bins=[number_of_bins, bins_y])

        return {
            "data": histogram.tolist(),
            "labels": [
                f"{format_evalue(math.exp(-x_edges[i]))} ≤ e-value < {format_evalue(math.exp(-x_edges[i + 1]))}"
                for i in range(number_of_bins)
            ],
            "categories": map(str.title, color_map.keys()),
            "colors": list(color_map.values()),
        }
