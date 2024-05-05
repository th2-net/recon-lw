from recon_lw.reporting.match_diff.categorizer import EventCategory
from recon_lw.reporting.match_diff.viewer.color_provider.base import ICategoryColorProvider


class DefaultCategoryColorProvider(ICategoryColorProvider):
    def get_category_color(self, category: EventCategory) -> str:
        # FIXME:
        #   It looks like this function should define color for every type of
        #   category
        #   Now it returns Puple -- we use it for known issues
        #   By default we usually use RED
        #   for WIP -- yellow

        return '#C3B1E1'  # Purple color.
