"""
Google Sheets API integration for reading and writing pipeline data.
Uses service account authentication for server-to-server access.
"""

from typing import Optional

import gspread
from google.oauth2.service_account import Credentials
import structlog

logger = structlog.get_logger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


class SheetsAPI:
    """
    Google Sheets integration for reading/writing pipeline results.

    Common use cases:
    - Writing scrape summaries to a reporting sheet
    - Reading configuration/URLs from a sheet
    - Exporting pipeline metrics for stakeholders

    Example:
        sheets = SheetsAPI(credentials_path="service-account.json")
        sheets.write_rows("spreadsheet_id", "Sheet1", data_rows)
    """

    def __init__(
        self,
        credentials_path: Optional[str] = None,
        credentials_dict: Optional[dict] = None,
    ):
        if credentials_dict:
            creds = Credentials.from_service_account_info(credentials_dict, scopes=SCOPES)
        elif credentials_path:
            creds = Credentials.from_service_account_file(credentials_path, scopes=SCOPES)
        else:
            raise ValueError("Provide either credentials_path or credentials_dict")

        self.client = gspread.authorize(creds)
        self.logger = structlog.get_logger(self.__class__.__name__)

    def get_sheet(self, spreadsheet_id: str, sheet_name: str) -> gspread.Worksheet:
        """Open a specific worksheet by spreadsheet ID and sheet name."""
        spreadsheet = self.client.open_by_key(spreadsheet_id)
        return spreadsheet.worksheet(sheet_name)

    def read_all(self, spreadsheet_id: str, sheet_name: str) -> list[dict]:
        """
        Read all rows from a sheet as list of dicts.
        First row is treated as headers.
        """
        sheet = self.get_sheet(spreadsheet_id, sheet_name)
        records = sheet.get_all_records()
        self.logger.info(
            "sheet_read",
            spreadsheet_id=spreadsheet_id,
            sheet=sheet_name,
            rows=len(records),
        )
        return records

    def read_column(self, spreadsheet_id: str, sheet_name: str, col: int = 1) -> list[str]:
        """Read all values from a specific column (1-indexed)."""
        sheet = self.get_sheet(spreadsheet_id, sheet_name)
        values = sheet.col_values(col)
        return [v for v in values if v]  # Filter empty cells

    def write_rows(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        rows: list[list],
        start_row: int = 1,
        clear_first: bool = False,
    ) -> None:
        """
        Write rows to a sheet.

        Args:
            spreadsheet_id: Google Spreadsheet ID
            sheet_name: Worksheet name
            rows: List of lists (each inner list is a row)
            start_row: Row number to start writing (1-indexed)
            clear_first: Clear existing content before writing
        """
        sheet = self.get_sheet(spreadsheet_id, sheet_name)

        if clear_first:
            sheet.clear()

        if rows:
            sheet.update(f"A{start_row}", rows)
            self.logger.info(
                "sheet_written",
                spreadsheet_id=spreadsheet_id,
                sheet=sheet_name,
                rows_written=len(rows),
            )

    def append_rows(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        rows: list[list],
    ) -> None:
        """Append rows to the end of a sheet."""
        sheet = self.get_sheet(spreadsheet_id, sheet_name)
        sheet.append_rows(rows, value_input_option="USER_ENTERED")
        self.logger.info(
            "rows_appended",
            spreadsheet_id=spreadsheet_id,
            sheet=sheet_name,
            count=len(rows),
        )

    def write_dataframe(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        data: list[dict],
        include_headers: bool = True,
    ) -> None:
        """
        Write a list of dicts to a sheet with automatic header generation.

        Args:
            spreadsheet_id: Google Spreadsheet ID
            sheet_name: Worksheet name
            data: List of dicts with consistent keys
            include_headers: Write column headers from dict keys
        """
        if not data:
            self.logger.warning("no_data_to_write")
            return

        headers = list(data[0].keys())
        rows = [[str(row.get(h, "")) for h in headers] for row in data]

        if include_headers:
            rows = [headers] + rows

        self.write_rows(spreadsheet_id, sheet_name, rows, clear_first=True)
