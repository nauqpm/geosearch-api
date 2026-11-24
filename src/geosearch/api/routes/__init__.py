"""FastAPI routers for GeoSearch API."""

from geosearch.api.routes.health import router as health_router
from geosearch.api.routes.places import router as places_router

__all__ = ["health_router", "places_router"]
