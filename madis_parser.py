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

# ── Station config ────────────────────────────────────────────────────────────
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

# ── In-memory store ───────────────────────────────────────────────────────────
live_data = {}
last_updated = None

def c_to_f(c):
    return round(c * 9/5 + 32, 1)

def get_netcdf_files_for_today():
    """Get list of netCDF filenames for today UTC"""
    now_utc = datetime.now(timezone.utc)
    files = []
    # Go back 12 hours to get all readings since local midnight
    for hours_back in range(13, -1, -1):
        dt = now_utc - timedelta(hours=hours_back)
        # Files are named YYYYMMDD_HHMM.gz, on the hour and half hour
        for minute in ['00', '20', '40']:
            fname = f"{dt.strftime('%Y%m%d_%H')}{minute}.gz"
            files.append(fname)
    return files

def parse_netcdf_simple(data):
    """
    Parse netCDF3 binary to extract station IDs and temperatures.
    netCDF3 classic format — we scan for station ID strings and nearby temp values.
    Returns list of {station, tempC, obs_time}
    """
    observations = []
    try:
        # netCDF3 magic: "CDF\x01" or "CDF\x02"
        if not data[:3] == b'CDF':
            log.warning("Not a netCDF3 file")
            return observations

        # Scan raw bytes for ICAO station identifiers (4 uppercase letters)
        # ASOS stations appear as 4-char strings in the data
        i = 0
        while i < len(data) - 100:
            # Look for 4-char uppercase airport codes
            chunk = data[i:i+4]
            try:
                code = chunk.decode('ascii')
                if (len(code) == 4 and code.isupper() and code.isalpha() 
                        and code in STATIONS):
                    # Found a station ID — look for temperature nearby
                    # Temperature in netCDF MADIS is stored as float32 in Kelvin or Celsius
                    # Search within next 200 bytes for a plausible temp value
                    for offset in range(4, 200, 4):
                        if i + offset + 4 > len(data):
                            break
                        try:
                            val = struct.unpack('>f', data[i+offset:i+offset+4])[0]
                            # Temp in Kelvin: 250-320K is -23°C to 47°C (reasonable)
                            if 250.0 <= val <= 320.0:
                                temp_c = val - 273.15
                                observations.append({
                                    'station': code,
                                    'tempC': temp_c,
                                    'tempF': c_to_f(temp_c),
                                })
                                break
                            # Temp in Celsius: -40 to 50°C
                            elif -40.0 <= val <= 50.0:
                                observations.append({
                                    'station': code,
                                    'tempC': val,
                                    'tempF': c_to_f(val),
                                })
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
    """Main fetch loop — downloads MADIS files and extracts temps"""
    global live_data, last_updated

    log.info("Starting MADIS fetch cycle...")

    # Track all readings per station since local midnight
    station_readings = {sta: [] for sta in STATIONS}

    files = get_netcdf_files_for_today()
    fetched = 0

    for fname in files:
        url = MADIS_BASE + fname
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'NowCast/1.0'})
            with urllib.request.urlopen(req, timeout=10) as resp:
                gz_data = resp.read()
                # Decompress gzip
                raw = gzip.decompress(gz_data)
                # Parse netCDF
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

    # Build result
    result = {}
    now_utc = datetime.now(timezone.utc)

    for sta, meta in STATIONS.items():
        readings = station_readings[sta]
        if readings:
            true_low = min(readings)
            true_high = max(readings)
            current = readings[-1] if readings else None
            result[sta] = {
                'station': sta,
                'city': meta['city'],
                'currentTempF': current,
                'trueLowF': true_low,
                'trueHighF': true_high,
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
    """Fetch every 5 minutes"""
    while True:
        try:
            fetch_and_parse()
        except Exception as e:
            log.error(f"Background loop error: {e}")
        time.sleep(300)  # 5 minutes

# ── Routes ────────────────────────────────────────────────────────────────────

@app.route('/liveobs')
def liveobs():
    return jsonify({
        'stations': live_data,
        'lastUpdated': last_updated,
        'stationCount': len(live_data),
    })

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
    return jsonify({
        'service': 'NowCast Live Obs',
        'endpoints': ['/liveobs', '/liveobs/<station>', '/health'],
        'stations': list(STATIONS.keys()),
    })

# ── Start ─────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    # Initial fetch on startup
    t = threading.Thread(target=background_loop, daemon=True)
    t.start()
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
