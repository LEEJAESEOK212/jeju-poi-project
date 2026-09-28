"""Recompute observed POI throughput; no API requests."""
import json
from datetime import datetime
from pathlib import Path

def calculate(data):
    result = {}
    for key in ('before', 'after'):
        row = data[key]
        seconds = (datetime.strptime(row['end'], '%H:%M:%S') - datetime.strptime(row['start'], '%H:%M:%S')).total_seconds()
        completed = row['completed_end'] - row['completed_start']
        assert seconds > 0 and completed > 0
        result[key] = {'elapsed_seconds': seconds, 'completed': completed,
                       'poi_per_minute': completed * 60 / seconds,
                       'aggregate_seconds_per_poi': seconds / completed}
    ratio = result['after']['poi_per_minute'] / result['before']['poi_per_minute']
    result['throughput_ratio'] = ratio
    result['throughput_increase_percent'] = (ratio - 1) * 100
    result['aggregate_seconds_per_poi_reduction_percent'] = (1 - 1 / ratio) * 100
    return result

if __name__ == '__main__':
    data = json.loads(Path(__file__).with_name('measurements.json').read_text(encoding='utf-8'))
    print(json.dumps(calculate(data), indent=2))
