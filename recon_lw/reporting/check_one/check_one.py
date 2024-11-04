import csv
from collections import defaultdict
from pathlib import Path
from typing import Callable, Tuple

from recon_lw.reporting.match_diff.categorizer.base import IErrorCategorizer
from th2_data_services.data import Data
class CheckOneReportGenerator:
    def __init__(
        self,
        output_path: Path,
        keep_matches: bool = False,
        examples_limit: int = 1000
    ):
        self.output_path = output_path
        self.keep_matches = keep_matches

    def generate_report(
        self,
        events: Data,
        key_function: Callable[[dict], str],
        timestamp_function: Callable[[dict], str],
        protocol_function: Callable[[dict], Tuple[str, str]]
    ):
        """
        Generates check-one like report for given events and configuration. It sorts columns from the one with most failes to the one without failes.

        One row for original stream message
        One row for copy stream message
        One row for comparison results

        Expected specific events format:
        {
          "body": {
             "match": [{'field': 'name', 'value': 'value'}, ...] - not required, but report will not be full without it.
             "diff": [{'field': 'name', 'expected': 'value1', 'actual': 'value2'}, ...] - required, but report will not be full without
          }
        }

        :param events: list of events
        :return:
        """
        all_fields_per_recon = defaultdict(set)
        field_failures_per_recon = defaultdict(lambda: defaultdict(int))
        field_presence_count = defaultdict(lambda: defaultdict(int))
        total_records_per_recon = defaultdict(int)
        data_per_recon = defaultdict(list)

        for event in events.filter(lambda e: e['eventType'] == 'BasicReconMatch'):
            recon_name = event['recon_name']
            event_body = event['body']

            diffs = event_body.get('diff', [])
            if not self.keep_matches and len(diffs) == 0:
                continue

            total_records_per_recon[recon_name] += 1

            matches = event_body.get('match', [])
            key = key_function(event)

            match_data = {}
            for match in matches:
                field = match['field']
                all_fields_per_recon[recon_name].add(field)
                field_presence_count[recon_name][field] += 1

                expected = match['expected']
                actual = match['expected']
                match_data[field] = {
                    'expected': str(expected),
                    'actual': str(actual),
                    'status': True
                }

            diff_data = {}
            for diff in diffs:
                field = diff['field']
                all_fields_per_recon[recon_name].add(field)
                field_presence_count[recon_name][field] += 1
                field_failures_per_recon[recon_name][field] += 1

                diff_data[field] = {
                    'expected': str(diff['expected']),
                    'actual': str(diff['actual']),
                    'status': False
                }

            combined_data = {**match_data, **diff_data}
            combined_data['stream_key'] = {
                'expected': str(key),
                'actual': str(key),
                'status': True
            }

            ts = timestamp_function(event)
            combined_data['timestamp'] = {
                'expected': str(ts),
                'actual': str(ts),
                'status': True
            }

            protocol_expected, protocol_actual = protocol_function(event)
            combined_data['protocol'] = {
                'expected': str(protocol_expected),
                'actual': str(protocol_actual),
                'status': True
            }

            data_per_recon[recon_name].append(combined_data)

        for recon_name, stats in data_per_recon.items():
            total_records = total_records_per_recon[recon_name]

            # Calculate missing field percentages
            missing_percentages = {
                field: ((total_records - field_presence_count[recon_name][field]) / total_records) * 100
                for field in all_fields_per_recon[recon_name]
            }

            # Sort fields by failures (descending), missing percentage (ascending), and field name
            sorted_fields = sorted(
                all_fields_per_recon[recon_name],
                key=lambda x: (
                    -field_failures_per_recon[recon_name][x],
                    missing_percentages[x],
                    x
                )
            )

            headers = ['protocol', 'status', 'stream_key', 'timestamp'] + sorted_fields

            rows = []
            for data in data_per_recon[recon_name]:
                overall_status = 'FAIL' if any([not value.get('status', True) for value in data.values()]) else 'PASS'
                data['status'] = {
                    'expected': overall_status,
                    'actual': overall_status,
                    'status': overall_status
                }
                rows.extend(
                    [
                        [data.get(key, {}).get('expected', '') for key in headers],
                        [data.get(key, {}).get('actual', '') for key in headers],
                        [data.get(key, {}).get('status', True) for key in headers]
                    ]
                )

                self.output_path.mkdir(parents=True, exist_ok=True)
                filename = f"{recon_name}_compare_rows.csv"

                output_file = self.output_path.joinpath(filename)

            with open(output_file, 'w', newline='', encoding='utf8') as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(rows)
            print(f'Output file generated: {output_file}')