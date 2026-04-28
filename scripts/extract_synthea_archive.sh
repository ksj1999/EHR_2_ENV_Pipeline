#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <archive.tar.gz> <output-dir>"
  echo "Example: $0 /data/synthea_1m_fhir_3_0_May_24.tar.gz /home/ec2-user/data_manipulation_project/data/synthea_bulk"
  exit 1
fi

ARCHIVE_PATH="$1"
OUTPUT_DIR="$2"

if [[ ! -f "$ARCHIVE_PATH" ]]; then
  echo "Archive not found: $ARCHIVE_PATH"
  exit 1
fi

mkdir -p "$OUTPUT_DIR"

echo "Extracting top-level archive into $OUTPUT_DIR"
tar -xzf "$ARCHIVE_PATH" -C "$OUTPUT_DIR"

extract_nested_archives() {
  local found=0

  while IFS= read -r -d '' file; do
    found=1
    case "$file" in
      *.tar.gz|*.tgz)
        local target_dir="${file%.tar.gz}"
        target_dir="${target_dir%.tgz}"
        mkdir -p "$target_dir"
        echo "Extracting nested archive: $file -> $target_dir"
        tar -xzf "$file" -C "$target_dir"
        rm -f "$file"
        ;;
      *.gz)
        echo "Decompressing nested gzip: $file"
        gunzip -f "$file"
        ;;
    esac
  done < <(find "$OUTPUT_DIR" -type f \( -name "*.tar.gz" -o -name "*.tgz" -o -name "*.gz" \) -print0)

  return $found
}

while extract_nested_archives; do
  :
done

echo
echo "Extraction complete. Looking for key Synthea CSV files..."
find "$OUTPUT_DIR" -type f \( -name "patients.csv" -o -name "conditions.csv" -o -name "encounters.csv" -o -name "observations.csv" \) | sort
