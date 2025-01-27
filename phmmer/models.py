from django.conf import settings

from search.models.job import HmmerJob
from pyhmmer.easel import TextSequence


class PhmmerJob(HmmerJob):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.algo = "PHMMER"

    def get_db_id(self):
        [db_config] = [config for config in settings.HMMER.databases if config.name == self.params["seqdb"]]
        if db_config is None:
            raise ValueError(f"No config found for {self.params['seqdb']}")
        return db_config.db

    def get_hmmpgmd_kwargs(self):
        kwargs = super().get_hmmpgmd_kwargs()
        kwargs["db"] = self.get_db_id()

        header, sequence = self._parse_fasta()
        print(f"'{header}'", f"'{sequence}'")
        kwargs["query"] = TextSequence(sequence=sequence, name=(header or "Query").encode())

        return kwargs

    def get_hmmpgmd_connection_params(self):
        [db_config] = [config for config in settings.HMMER.databases if config.name == self.params["seqdb"]]
        if db_config is None:
            raise ValueError(f"No config found for {self.params['seqdb']}")
        return {
            "address": db_config.host,
            "port": db_config.port,
        }

    def _parse_fasta(self):
        if not self.params["seq"]:
            return None, None

        lines = self.params["seq"].strip().split("\n")

        if not lines:
            return None, None

        # Check if it's in FASTA format
        if lines[0].startswith(">"):
            header = lines[0][1:].strip()  # Remove '>' and whitespace
            sequence = "".join(line.strip() for line in lines[1:])
            return header, sequence

        # If not in FASTA format, treat as raw sequence
        return None, "".join(line.strip() for line in lines)

    class Meta:
        proxy = True
