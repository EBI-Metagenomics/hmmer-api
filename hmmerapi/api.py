from ninja import NinjaAPI
from search.utils import OrjsonRenderer

api = NinjaAPI(version="1.0.0", renderer=OrjsonRenderer())

api.add_router("/search", "search.api.router")
api.add_router("/search/phmmer", "phmmer.api.router")
api.add_router("/result", "result.api.router")
api.add_router("/taxonomy", "taxonomy.api.router")
