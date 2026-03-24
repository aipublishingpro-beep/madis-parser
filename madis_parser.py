"""
MADIS 1-Minute ASOS Parser for NowCast
Runs on Railway — fetches MADIS HFMETAR netCDF files every 5 minutes
Exposes /liveobs endpoint with true low/high per station
"""

import os
import gzip
import struct
import urllib.request
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify
import threading
import time
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = Flask(__name__)

STATIONS = {
    'KNYC': {'city': 'New York',       'tz_offset': -5},
    'KMIA': {'city': 'Miami',          'tz_offset': -5},
    'KLAX': {'city': 'Los Angeles',    'tz_offset': -8},
    'KAUS': {'city': 'Austin',         'tz_offset': -6},
    'KMDW': {'city': 'Chicago',        'tz_offset': -6},
    'KDEN': {'city': 'Denver',         'tz_offset': -7},
    'KPHL': {'city': 'Philadelphia',   'tz_offset': -5},
    'KIAH': {'city': 'Houston',        'tz_offset': -6},
    'KLAS': {'city': 'Las Vegas',      'tz_offset': -8},
    'KSEA': {'city': 'Seattle',        'tz_offset': -8},
    'KSFO': {'city': 'San Francisco',  'tz_offset': -8},
    'KDCA': {'city': 'Washington DC',  'tz_offset': -5},
    'KMSY': {'city': 'New Orleans',    'tz_offset': -6},
    'KPHX': {'city': 'Phoenix',        'tz_offset': -7},
    'KATL': {'city': 'Atlanta',        'tz_offset': -5},
    'KMSP': {'city': 'Minneapolis',    'tz_offset': -6},
    'KBOS': {'city': 'Boston',         'tz_offset': -5},
    'KOKC': {'city': 'Oklahoma City',  'tz_offset': -6},
}

MADIS_BASE = 'https://madis-data.ncep.noaa.gov/madisPublic1/data/LDAD/hfmetar/netCDF/'

live_data = {}
last_updated = None

def c_to_f(c):
    return round(c * 9/5 + 32, 1)

def get_netcdf_files_for_today():
    now_utc = datetime.now(timezone.utc)
    files = []
    for hours_back in range(13, -1, -1):
        dt = now_utc - timedelta(hours=hours_back)
        for minute in ['00', '20', '40']:
            fname = f"{dt.strftime('%Y%m%d_%H')}{minute}.gz"
            files.append(fname)
    return files

def parse_netcdf_simple(data):
    observations = []
    try:
        if not data[:3] == b'CDF':
            log.warning("Not a netCDF3 file")
            return observations
        i = 0
        while i < len(data) - 100:
            chunk = data[i:i+4]
            try:
                code = chunk.decode('ascii')
                if (len(code) == 4 and code.isupper() and code.isalpha()
                        and code in STATIONS):
                    for offset in range(4, 200, 4):
                        if i + offset + 4 > len(data):
                            break
                        try:
                            val = struct.unpack('>f', data[i+offset:i+offset+4])[0]
                            if 250.0 <= val <= 320.0:
                                temp_c = val - 273.15
                                observations.append({'station': code, 'tempF': c_to_f(temp_c)})
                                break
                            elif -40.0 <= val <= 50.0:
                                observations.append({'station': code, 'tempF': c_to_f(val)})
                                break
                        except:
                            pass
            except:
                pass
            i += 1
    except Exception as e:
        log.error(f"netCDF parse error: {e}")
    return observations

def fetch_and_parse():
    global live_data, last_updated
    log.info("Starting MADIS fetch cycle...")
    station_readings = {sta: [] for sta in STATIONS}
    files = get_netcdf_files_for_today()
    fetched = 0
    for fname in files:
        url = MADIS_BASE + fname
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'NowCast/1.0'})
            with urllib.request.urlopen(req, timeout=10) as resp:
                gz_data = resp.read()
                raw = gzip.decompress(gz_data)
                obs = parse_netcdf_simple(raw)
                for o in obs:
                    sta = o['station']
                    if sta in station_readings:
                        station_readings[sta].append(o['tempF'])
                fetched += 1
                log.info(f"Fetched {fname}: {len(obs)} obs")
        except urllib.error.HTTPError as e:
            if e.code != 404:
                log.warning(f"{fname}: HTTP {e.code}")
        except Exception as e:
            log.warning(f"{fname}: {e}")
    log.info(f"Fetched {fetched} files")
    result = {}
    now_utc = datetime.now(timezone.utc)
    for sta, meta in STATIONS.items():
        readings = station_readings[sta]
        if readings:
            result[sta] = {
                'station': sta,
                'city': meta['city'],
                'currentTempF': readings[-1],
                'trueLowF': min(readings),
                'trueHighF': max(readings),
                'readingCount': len(readings),
                'fetchedAt': now_utc.isoformat(),
            }
        else:
            result[sta] = {
                'station': sta,
                'city': meta['city'],
                'currentTempF': None,
                'trueLowF': None,
                'trueHighF': None,
                'readingCount': 0,
                'fetchedAt': now_utc.isoformat(),
                'error': 'No data',
            }
    live_data = result
    last_updated = now_utc.isoformat()
    log.info(f"Live data updated: {len(result)} stations")

def background_loop():
    while True:
        try:
            fetch_and_parse()
        except Exception as e:
            log.error(f"Background loop error: {e}")
        time.sleep(300)

# Start background thread at module level — works with gunicorn
t = threading.Thread(target=background_loop, daemon=True)
t.start()

@app.route('/liveobs')
def liveobs():
    return jsonify({'stations': live_data, 'lastUpdated': last_updated, 'stationCount': len(live_data)})

@app.route('/liveobs/<station>')
def liveobs_station(station):
    sta = station.upper()
    if sta in live_data:
        return jsonify(live_data[sta])
    return jsonify({'error': 'Station not found'}), 404

@app.route('/health')
def health():
    return jsonify({'status': 'ok', 'lastUpdated': last_updated})

@app.route('/')
def index():
    return jsonify({'service': 'NowCast Live Obs', 'endpoints': ['/liveobs', '/liveobs/<station>', '/health'], 'stations': list(STATIONS.keys())})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
