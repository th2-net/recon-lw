from recon_lw.reporting.match_diff.viewer.color_provider.base import ICategoryColorProvider


class DefaultCategoryColorProvider(ICategoryColorProvider):
    def get_category_color(self, category: str) -> str:
        return '#C3B1E1'