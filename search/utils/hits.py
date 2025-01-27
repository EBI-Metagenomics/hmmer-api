def hits_to_json(hits: TopHits) -> str:
    return json.dumps(hits, cls=PyHMMEREncoder)
