"""
MADIS 1-Minute ASOS Parser for NowCast
Uses netCDF4 library for proper parsing
"""

import os
import gzip
import tempfile
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
    'KLGA': {'city': 'New York'},
    'KMIA': {'city': 'Miami'},
    'KLAX': {'city': 'Los Angeles'},
    'KAUS': {'city': 'Austin'},
    'KMDW': {'city': 'Chicago'},
    'KDEN': {'city': 'Denver'},
    'KPHL': {'city': 'Philadelphia'},
    'KIAH': {'city': 'Houston'},
    'KLAS': {'city': 'Las Vegas'},
    'KSEA': {'city': 'Seattle'},
    'KSFO': {'city': 'San Francisco'},
    'KDCA': {'city': 'Washington DC'},
    'KMSY': {'city': 'New Orleans'},
    'KPHX': {'city': 'Phoenix'},
    'KATL': {'city': 'Atlanta'},
    'KMSP': {'city': 'Minneapolis'},
    'KBOS': {'city': 'Boston'},
    'KOKC': {'city': 'Oklahoma City'},
}

MADIS_BASE = 'https://madis-data.ncep.noaa.gov/madisPublic1/data/LDAD/hfmetar/netCDF/'

live_data = {}
last_updated = None


def c_to_f(c):
    return round(float(c) * 9/5 + 32, 1)

def k_to_f(k):
    return round((float(k) - 273.15) * 9/5 + 32, 1)


def parse_madis_netcdf(filepath):
    """Parse a MADIS HFMETAR netCDF file and return {station: [tempF]} dict"""
    import netCDF4 as nc
    import numpy as np

    results = {}
    try:
        ds = nc.Dataset(filepath, 'r')

        # Log available variables on first parse
        log.info(f"NetCDF vars: {list(ds.variables.keys())}")

        # Get station IDs
        station_var = None
        for vname in ['stationId', 'stationName', 'station_id', 'wmoStaNum', 'stationIDchar']:
            if vname in ds.variables:
                station_var = vname
                break

        # Get temperature variable
        temp_var = None
        for vname in ['temperature', 'airTemperature', 'temp', 'T', 'air_temp', 'Temperature']:
            if vname in ds.variables:
                temp_var = vname
                break

        log.info(f"Using station_var={station_var} temp_var={temp_var}")

        if not station_var or not temp_var:
            log.warning(f"Missing variables. Available: {list(ds.variables.keys())}")
            ds.close()
            return results

        # Read station IDs
        raw_ids = ds.variables[station_var][:]
        temps = ds.variables[temp_var][:]

        # Handle masked arrays
        if hasattr(temps, 'data'):
            temps_data = temps.data
            temps_mask = temps.mask if hasattr(temps, 'mask') else np.zeros_like(temps_data, dtype=bool)
        else:
            temps_data = np.array(temps)
            temps_mask = np.zeros_like(temps_data, dtype=bool)

        # Decode station IDs
        nrecs = raw_ids.shape[0]
        for i in range(nrecs):
            try:
                # Station ID can be char array or string
                raw = raw_ids[i]
                if hasattr(raw, 'tobytes'):
                    sid = raw.tobytes().decode('ascii', errors='replace').strip('\x00').strip()
                elif hasattr(raw, '__iter__'):
                    sid = ''.join(chr(int(c)) for c in raw if int(c) > 0).strip()
                else:
                    sid = str(raw).strip()

                if not sid or sid not in STATIONS:
                    continue

                # Get temperature
                if temps_mask is not None and np.ndim(temps_mask) > 0:
                    if temps_mask[i]:
                        continue
                
                tempK = float(temps_data[i])
                if abs(tempK) > 1e10 or tempK != tempK:
                    continue

                # Convert to F
                if 200 <= tempK <= 350:
                    tempF = k_to_f(tempK)
                elif -60 <= tempK <= 60:
                    tempF = c_to_f(tempK)
                else:
                    continue

                if -60 <= tempF <= 140:
                    if sid not in results:
                        results[sid] = []
                    results[sid].append(tempF)

            except Exception as e:
                continue

        ds.close()
        log.info(f"Parsed {sum(len(v) for v in results.values())} readings for {len(results)} stations")

    except Exception as e:
        log.error(f"netCDF4 parse error: {e}")

    return results


def get_files_for_today():
    now_utc = datetime.now(timezone.utc)
    files = []
    for hours_back in range(13, -1, -1):
        dt = now_utc - timedelta(hours=hours_back)
        for minute in ['00', '20', '40']:
            fname = f"{dt.strftime('%Y%m%d_%H')}{minute}.gz"
            files.append(fname)
    return files


def fetch_and_parse():
    global live_data, last_updated
    log.info("Starting MADIS fetch cycle...")

    station_readings = {sta: [] for sta in STATIONS}
    files = get_files_for_today()
    fetched = 0

    for fname in files:
        url = MADIS_BASE + fname
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'NowCast/1.0'})
            with urllib.request.urlopen(req, timeout=15) as resp:
                gz_data = resp.read()

            # Decompress to temp file (netCDF4 needs a file path)
            with tempfile.NamedTemporaryFile(suffix='.nc', delete=False) as tmp:
                tmp.write(gzip.decompress(gz_data))
                tmp_path = tmp.name

            obs = parse_madis_netcdf(tmp_path)
            os.unlink(tmp_path)

            for sta, temps in obs.items():
                station_readings[sta].extend(temps)
            fetched += 1

        except urllib.error.HTTPError as e:
            if e.code != 404:
                log.warning(f"{fname}: HTTP {e.code}")
        except Exception as e:
            log.warning(f"{fname}: {e}")

    log.info(f"Fetched {fetched} files")
    for sta, readings in station_readings.items():
        if readings:
            log.info(f"{sta}: {len(readings)} readings low={min(readings):.1f} high={max(readings):.1f} current={readings[-1]:.1f}")

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
    log.info(f"Done. {len([r for r in result.values() if r.get('currentTempF')])} stations with data.")


def background_loop():
    while True:
        try:
            fetch_and_parse()
        except Exception as e:
            log.error(f"Background loop error: {e}")
        time.sleep(300)


# Start at module level for gunicorn
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
