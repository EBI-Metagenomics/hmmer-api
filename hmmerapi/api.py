from ninja import NinjaAPI

api = NinjaAPI(version="1.0.0")

api.add_router("/architecture", "architecture.api.router")
api.add_router("/result", "result.api.router")
api.add_router("/search", "search.api.router")
api.add_router("/taxonomy", "taxonomy.api.router")
api.add_router("/download", "download.api.router")
