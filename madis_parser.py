"""
MADIS 1-Minute ASOS Parser for NowCast
Proper netCDF3 classic format parser
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
    'KNYC': {'city': 'New York'},
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

STATION_ALIASES = {
    'NYC':  'KNYC',
    'LGA':  'KNYC',
    'KLGA': 'KNYC',
}

MADIS_BASE = 'https://madis-data.ncep.noaa.gov/madisPublic1/data/LDAD/hfmetar/netCDF/'

live_data = {}
last_updated = None


def read_int(data, offset):
    return struct.unpack('>i', data[offset:offset+4])[0], offset + 4

def read_string(data, offset, length):
    s = data[offset:offset+length]
    padded = length + (4 - length % 4) % 4
    return s.decode('ascii', errors='replace').strip('\x00').strip(), offset + padded

def parse_netcdf3(data):
    results = {}
    try:
        if data[:3] != b'CDF':
            return results

        version = data[3]
        offset = 4
        numrecs, offset = read_int(data, offset)

        # dim_list
        tag, offset = read_int(data, offset)
        if tag != 10:
            return results
        ndims, offset = read_int(data, offset)
        dims = {}
        for i in range(ndims):
            nl, offset = read_int(data, offset)
            name, offset = read_string(data, offset, nl)
            dl, offset = read_int(data, offset)
            dims[i] = {'name': name, 'len': dl}

        # global att_list
        tag, offset = read_int(data, offset)
        if tag == 0:
            offset += 4
        elif tag == 12:
            natts, offset = read_int(data, offset)
            for _ in range(natts):
                nl, offset = read_int(data, offset)
                _, offset = read_string(data, offset, nl)
                nc_type, offset = read_int(data, offset)
                nelems, offset = read_int(data, offset)
                size = {1:1,2:1,3:2,4:4,5:8,6:4}.get(nc_type, 4)
                total = nelems * size
                padded = total + (4 - total % 4) % 4
                offset += padded

        # var_list
        tag, offset = read_int(data, offset)
        variables = {}
        if tag == 11:
            nvars, offset = read_int(data, offset)
            for _ in range(nvars):
                nl, offset = read_int(data, offset)
                name, offset = read_string(data, offset, nl)
                ndimids, offset = read_int(data, offset)
                dimids = []
                for _ in range(ndimids):
                    did, offset = read_int(data, offset)
                    dimids.append(did)
                # var atts
                att_tag, offset = read_int(data, offset)
                if att_tag == 0:
                    offset += 4
                elif att_tag == 12:
                    natts, offset = read_int(data, offset)
                    for _ in range(natts):
                        anl, offset = read_int(data, offset)
                        _, offset = read_string(data, offset, anl)
                        nc_type2, offset = read_int(data, offset)
                        nelems2, offset = read_int(data, offset)
                        size = {1:1,2:1,3:2,4:4,5:8,6:4}.get(nc_type2, 4)
                        total = nelems2 * size
                        padded = total + (4 - total % 4) % 4
                        offset += padded
                nc_type3, offset = read_int(data, offset)
                vsize, offset = read_int(data, offset)
                if version == 2:
                    begin = struct.unpack('>q', data[offset:offset+8])[0]
                    offset += 8
                else:
                    begin, offset = read_int(data, offset)
                variables[name] = {'dimids': dimids, 'nc_type': nc_type3, 'vsize': vsize, 'begin': begin}

        log.info(f"Variables found: {list(variables.keys())}")

        # Find station and temp variables
        station_var = next((v for v in ['stationId','stationName','station_id'] if v in variables), None)
        temp_var = next((v for v in ['temperature','airTemperature','temp','T'] if v in variables), None)

        if not station_var or not temp_var:
            log.warning(f"Missing vars — station={station_var} temp={temp_var}")
            return results

        sv = variables[station_var]
        tv = variables[temp_var]
        nrecs = numrecs if numrecs > 0 else 1

        # String length for station IDs
        str_len = dims.get(sv['dimids'][1] if len(sv['dimids']) > 1 else 0, {}).get('len', 8)

        # Read station IDs
        station_ids = []
        for i in range(nrecs):
            s_off = sv['begin'] + i * str_len
            raw = data[s_off:s_off+str_len]
            sid = raw.decode('ascii', errors='replace').strip('\x00').strip()
            station_ids.append(sid)

        # Read temperatures
        temps = []
        for i in range(nrecs):
            t_off = tv['begin'] + i * 4
            try:
                val = struct.unpack('>f', data[t_off:t_off+4])[0]
                if abs(val) > 1e10 or val != val:
                    temps.append(None)
                else:
                    temps.append(val)
            except:
                temps.append(None)

        # Match and convert
        for sid, tempK in zip(station_ids, temps):
            if not sid or tempK is None:
                continue
            sta = STATION_ALIASES.get(sid, sid)
            if sta not in STATIONS:
                continue
            if 200 <= tempK <= 350:
                tempF = round((tempK - 273.15) * 9/5 + 32, 1)
            elif -60 <= tempK <= 60:
                tempF = round(tempK * 9/5 + 32, 1)
            else:
                continue
            if sta not in results:
                results[sta] = []
            results[sta].append(tempF)

    except Exception as e:
        log.error(f"netCDF3 parse error: {e}")

    return results


def get_netcdf_files_for_today():
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
    files = get_netcdf_files_for_today()
    fetched = 0

    for fname in files:
        url = MADIS_BASE + fname
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'NowCast/1.0'})
            with urllib.request.urlopen(req, timeout=15) as resp:
                gz_data = resp.read()
            raw = gzip.decompress(gz_data)
            obs = parse_netcdf3(raw)
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
            log.info(f"{sta}: {len(readings)} readings low={min(readings)} high={max(readings)}")

    result = {}
    now_utc = datetime.now(timezone.utc)
    for sta, meta in STATIONS.items():
        readings = station_readings[sta]
        if readings:
            result[sta] = {'station': sta, 'city': meta['city'], 'currentTempF': readings[-1], 'trueLowF': min(readings), 'trueHighF': max(readings), 'readingCount': len(readings), 'fetchedAt': now_utc.isoformat()}
        else:
            result[sta] = {'station': sta, 'city': meta['city'], 'currentTempF': None, 'trueLowF': None, 'trueHighF': None, 'readingCount': 0, 'fetchedAt': now_utc.isoformat(), 'error': 'No data'}

    live_data = result
    last_updated = now_utc.isoformat()


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
