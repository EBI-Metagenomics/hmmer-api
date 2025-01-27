import orjson
from ninja.renderers import BaseRenderer
from pyhmmer.plan7 import TopHits, Hit, Domains, Domain, Alignment


class OrjsonRenderer(BaseRenderer):
    media_type = "application/json"

    def render(self, request, data, *, response_status):
        return orjson.dumps(data, default=OrjsonRenderer._default)

    @classmethod
    def _default(cls, obj):
        if isinstance(obj, TopHits):
            return {
                "T": obj.T,
                "E": obj.E,
                "Z": obj.Z,
                "bit_cutoffs": obj.bit_cutoffs,
                "block_length": obj.block_length,
                "domE": obj.domE,
                "domT": obj.domT,
                "domZ": obj.domZ,
                "incE": obj.incE,
                "incT": obj.incT,
                "incdomE": obj.incdomE,
                "incdomT": obj.incdomT,
                "included": list(obj.included),
                "long_targets": obj.long_targets,
                "mode": obj.mode,
                "query": obj.query,
                # "query_accession": obj.query_accession,
                # "query_length": obj.query_length,
                # "query_name": obj.query_name,
                "reported": list(obj.reported),
                "searched_models": obj.searched_models,
                "searched_nodes": obj.searched_nodes,
                "searched_residues": obj.searched_residues,
                "searched_sequences": obj.searched_sequences,
                "strand": obj.strand,
            }
        if isinstance(obj, Hit):
            return {
                "accession": obj.accession.decode() if obj.accession else None,
                "best_domain": obj.best_domain,
                "bias": obj.bias,
                "description": obj.description.decode() if obj.description else None,
                "domains": obj.domains,
                "dropped": obj.dropped,
                "duplicate": obj.duplicate,
                "evalue": obj.evalue,
                "included": obj.included,
                "length": obj.length,
                "name": obj.name.decode() if obj.name else None,
                "new": obj.new,
                "pre_score": obj.pre_score,
                "pvalue": obj.pvalue,
                "reported": obj.reported,
                "score": obj.score,
                "sum_score": obj.sum_score,
            }
        if isinstance(obj, Domains):
            return {
                "included": list(obj.included),
                "reported": list(obj.reported),
            }

        if isinstance(obj, Domain):
            return {
                "alignment": obj.alignment,
                "bias": obj.bias,
                "c_evalue": obj.c_evalue,
                "correction": obj.correction,
                "env_from": obj.env_from,
                "env_to": obj.env_to,
                "envelope_score": obj.envelope_score,
                "i_evalue": obj.i_evalue,
                "included": obj.included,
                "pvalue": obj.pvalue,
                "reported": obj.reported,
                "score": obj.score,
                "strand": obj.strand,
            }
        if isinstance(obj, Alignment):
            return {
                "hmm_accession": (
                    obj.hmm_accession.decode() if obj.hmm_accession else None
                ),
                "hmm_from": obj.hmm_from,
                "hmm_length": obj.hmm_length,
                "hmm_name": obj.hmm_name.decode() if obj.hmm_name else None,
                "hmm_sequence": obj.hmm_sequence,
                "hmm_to": obj.hmm_to,
                "identity_sequence": obj.identity_sequence,
                "posterior_probabilities": obj.posterior_probabilities,
                "target_from": obj.target_from,
                "target_length": obj.target_length,
                "target_name": obj.target_name.decode() if obj.target_name else None,
                "target_sequence": obj.target_sequence,
                "target_to": obj.target_to,
            }
        raise TypeError
