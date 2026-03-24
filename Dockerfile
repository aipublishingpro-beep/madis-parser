FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    libhdf5-dev \
    libnetcdf-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements_full.txt .
RUN pip install flask gunicorn netCDF4 numpy

COPY madis_parser.py .
CMD gunicorn madis_parser:app --bind 0.0.0.0:$PORT --workers 1 --threads 4
