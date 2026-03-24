"""
MADIS 1-Minute ASOS Parser for NowCast
LOW reversal detection using 1-minute data
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
    'KLGA': {'city': 'New York',       'tz': 'America/New_York',    'tz_offset': -4},
    'KMIA': {'city': 'Miami',          'tz': 'America/New_York',    'tz_offset': -4},
    'KLAX': {'city': 'Los Angeles',    'tz': 'America/Los_Angeles', 'tz_offset': -7},
    'KAUS': {'city': 'Austin',         'tz': 'America/Chicago',     'tz_offset': -5},
    'KMDW': {'city': 'Chicago',        'tz': 'America/Chicago',     'tz_offset': -5},
    'KDEN': {'city': 'Denver',         'tz': 'America/Denver',      'tz_offset': -6},
    'KPHL': {'city': 'Philadelphia',   'tz': 'America/New_York',    'tz_offset': -4},
    'KIAH': {'city': 'Houston',        'tz': 'America/Chicago',     'tz_offset': -5},
    'KLAS': {'city': 'Las Vegas',      'tz': 'America/Los_Angeles', 'tz_offset': -7},
    'KSEA': {'city': 'Seattle',        'tz': 'America/Los_Angeles', 'tz_offset': -7},
    'KSFO': {'city': 'San Francisco',  'tz': 'America/Los_Angeles', 'tz_offset': -7},
    'KDCA': {'city': 'Washington DC',  'tz': 'America/New_York',    'tz_offset': -4},
    'KMSY': {'city': 'New Orleans',    'tz': 'America/Chicago',     'tz_offset': -5},
    'KPHX': {'city': 'Phoenix',        'tz': 'America/Phoenix',     'tz_offset': -7},
    'KATL': {'city': 'Atlanta',        'tz': 'America/New_York',    'tz_offset': -4},
    'KMSP': {'city': 'Minneapolis',    'tz': 'America/Chicago',     'tz_offset': -5},
    'KBOS': {'city': 'Boston',         'tz': 'America/New_York',    'tz_offset': -4},
    'KOKC': {'city': 'Oklahoma City',  'tz': 'America/Chicago',     'tz_offset': -5},
}

MADIS_BASE = 'https://madis-data.ncep.noaa.gov/madisPublic1/data/LDAD/hfmetar/netCDF/'

live_data = {}
last_updated = None

def c_to_f(c):
    return round(float(c) * 9/5 + 32, 1)

def k_to_f(k):
    return round((float(k) - 273.15) * 9/5 + 32, 1)

def detect_low_reversal(readings_with_time):
    """
    Detect LOW reversal from list of (tempF, utc_epoch) tuples sorted oldest→newest.
    Returns: {
        trueLowF, trueLowTime (ISO), trueLowConfirmed,
        reversalTempF, reversalTime (ISO)
    }
    Confirmed = 3 consecutive readings rising after the bottom.
    """
    if not readings_with_time:
        return None

    # Find the minimum temp and its index
    min_temp = None
    min_idx = 0
    for i, (t, _) in enumerate(readings_with_time):
        if min_temp is None or t < min_temp:
            min_temp = t
            min_idx = i

    min_time = readings_with_time[min_idx][1]

    # Check for 3 consecutive rising readings after the minimum
    confirmed = False
    reversal_temp = None
    reversal_time = None

    after_min = readings_with_time[min_idx+1:]
    consecutive_rises = 0
    prev_temp = min_temp

    for temp, ts in after_min:
        if temp > prev_temp:
            consecutive_rises += 1
            if consecutive_rises >= 3:
                confirmed = True
                reversal_temp = temp
                reversal_time = ts
                break
        else:
            consecutive_rises = 0
        prev_temp = temp

    return {
        'trueLowF': round(min_temp),
        'trueLowTempRaw': min_temp,
        'trueLowTime': datetime.fromtimestamp(min_time, tz=timezone.utc).isoformat() if min_time else None,
        'trueLowConfirmed': confirmed,
        'reversalTempF': round(reversal_temp) if reversal_temp else None,
        'reversalTime': datetime.fromtimestamp(reversal_time, tz=timezone.utc).isoformat() if reversal_time else None,
        'readingCount': len(readings_with_time),
    }


def parse_madis_netcdf(filepath):
    """Parse MADIS HFMETAR netCDF — returns {station: [(tempF, obs_epoch)]}"""
    import netCDF4 as nc
    import numpy as np

    results = {}
    try:
        ds = nc.Dataset(filepath, 'r')

        station_var = next((v for v in ['stationId','stationName','station_id'] if v in ds.variables), None)
        temp_var = next((v for v in ['temperature','airTemperature','temp'] if v in ds.variables), None)
        time_var = next((v for v in ['observationTime','obsTime','time'] if v in ds.variables), None)

        if not station_var or not temp_var:
            ds.close()
            return results

        raw_ids = ds.variables[station_var][:]
        temps = ds.variables[temp_var][:]
        times = ds.variables[time_var][:] if time_var else None

        if hasattr(temps, 'data'):
            temps_data = temps.data
            temps_mask = temps.mask if hasattr(temps, 'mask') and np.ndim(temps.mask) > 0 else None
        else:
            temps_data = np.array(temps)
            temps_mask = None

        nrecs = raw_ids.shape[0]
        for i in range(nrecs):
            try:
                raw = raw_ids[i]
                if hasattr(raw, 'tobytes'):
                    sid = raw.tobytes().decode('ascii', errors='replace').strip('\x00').strip()
                elif hasattr(raw, '__iter__'):
                    sid = ''.join(chr(int(c)) for c in raw if int(c) > 0).strip()
                else:
                    sid = str(raw).strip()

                if not sid or sid not in STATIONS:
                    continue

                if temps_mask is not None and temps_mask[i]:
                    continue

                tempK = float(temps_data[i])
                if abs(tempK) > 1e10 or tempK != tempK:
                    continue

                if 200 <= tempK <= 350:
                    tempF = k_to_f(tempK)
                elif -60 <= tempK <= 60:
                    tempF = c_to_f(tempK)
                else:
                    continue

                if not (-60 <= tempF <= 140):
                    continue

                obs_epoch = None
                if times is not None:
                    try:
                        obs_epoch = float(times[i])
                    except:
                        pass

                if sid not in results:
                    results[sid] = []
                results[sid].append((tempF, obs_epoch))

            except:
                continue

        ds.close()

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

            with tempfile.NamedTemporaryFile(suffix='.nc', delete=False) as tmp:
                tmp.write(gzip.decompress(gz_data))
                tmp_path = tmp.name

            obs = parse_madis_netcdf(tmp_path)
            os.unlink(tmp_path)

            for sta, readings in obs.items():
                station_readings[sta].extend(readings)
            fetched += 1

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
            # Sort by time
            readings_sorted = sorted(readings, key=lambda x: x[1] if x[1] else 0)
            temps_only = [r[0] for r in readings_sorted]

            current = temps_only[-1] if temps_only else None
            true_high = max(temps_only)
            true_low_raw = min(temps_only)

            # LOW reversal detection
            low_reversal = detect_low_reversal(readings_sorted)

            result[sta] = {
                'station': sta,
                'city': meta['city'],
                'currentTempF': round(current) if current else None,
                'currentTempRaw': current,
                'trueLowF': round(true_low_raw),
                'trueLowRaw': true_low_raw,
                'trueHighF': round(true_high),
                'trueHighRaw': true_high,
                'readingCount': len(readings),
                'fetchedAt': now_utc.isoformat(),
                'lowReversal': low_reversal,
            }

            if low_reversal:
                log.info(f"{sta}: low={true_low_raw:.1f}F high={true_high:.1f}F confirmed={low_reversal['trueLowConfirmed']}")
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
                'lowReversal': None,
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
