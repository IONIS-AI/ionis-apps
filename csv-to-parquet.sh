#!/bin/bash

export PATH="$HOME/.duckdb/cli/latest:$PATH"

# Directory containing your CSVs
IN_DIR="/mnt/wspr-data"

# Directory where you want the Parquets
OUT_DIR="/mnt/wspr-data/parquet"

# Create output dir if it doesn't exist
mkdir -p "$OUT_DIR"

# Loop through all CSV files
for csv_file in "$IN_DIR"/*.csv; do
    # Get the filename without extension
    base_name=$(basename "$csv_file" .csv)
    
    echo "Processing $base_name ..."
    
    duckdb -c "
    SET preserve_insertion_order = false;
    SET threads = 32;
    COPY (
        SELECT * FROM read_csv_auto('$csv_file')
    ) TO '$OUT_DIR/$base_name.parquet' 
    (FORMAT PARQUET, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 1048576);"
done

echo "Conversion Complete!"