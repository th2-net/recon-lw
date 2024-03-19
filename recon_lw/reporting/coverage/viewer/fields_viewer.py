from dataclasses import asdict
from typing import Dict, List, Any
import textwrap as tw

from IPython.core.display import HTML
from IPython.core.display_functions import display

from recon_lw.reporting.recon_context.context import ReconContext
from recon_lw.reporting.recon_metadata.base import ReconMetadata

ROW_DEFAULT_STYLE_CONFIG = "text-align: left"
class FieldsTableStyleConfig:
    def __init__(
            self,
            row_style: str=ROW_DEFAULT_STYLE_CONFIG,
            text_field_width: int = 100,
            row_font_size: int=15
    ):
        self.row_style = row_style
        self.row_font_size=row_font_size
        self.text_field_width = text_field_width

class FieldsTableViewer:
    @staticmethod
    def get_fields_table(
            table_name: str,
            fields: List[Dict[str, Any]],
            style_config: FieldsTableStyleConfig
    ):
        columns = list(fields[0].keys())

        def get_row(field: dict):
            row = '<tr>'
            for field in field.values():
                row += f'\n<td style="{style_config.row_style}">{FieldsViewerUtils.wrap_text(field, style_config)}</td>'
            row += "</tr>"
            return row

        table = f"""
        <table border="0" width="100%">
        <tr>
            <td style="text-align: left"><b style="font-size: 20px;">{table_name}</b></td>
        </tr>
        """
        table += "<tr>"
        for column in columns:
            table += (f'<td style="{style_config.row_style}">'
                      f'<b style="font-size: {style_config.row_font_size}">{column}</td>')
        table += "</tr>"

        table += "\n".join(
            [get_row(item) for item in fields]
        )

        table += "</table>"

        return table

class ReconMetadataFieldsViewer:
    def __init__(self,
                 recon_context: ReconContext,
                 styles: FieldsTableStyleConfig = FieldsTableStyleConfig()
    ):
        self.recon_context = recon_context
        self.styles = styles

    def display(self):
        for recon_name, metadata in self.recon_context.get_metadata().items():
            fields = list(map(lambda x: asdict(x), metadata.covered_fields))
            display(HTML(FieldsTableViewer.get_fields_table(recon_name, fields, self.styles)))

class FieldsViewerUtils:
    @staticmethod
    def wrap_text(text: str, style: FieldsTableStyleConfig) -> str:
        return "<br/>".join(tw.wrap(text, width=style.text_field_width))