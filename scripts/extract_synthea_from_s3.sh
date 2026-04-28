#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <s3-uri-or-https-url> <output-dir>"
  echo "Example: $0 s3://synthea-full-bucket/synthea_1m_fhir_3_0_May_24.tar /home/ec2-user/data_manipulation_project/data/synthea_bulk"
  exit 1
fi

SOURCE="$1"
OUTPUT_DIR="$2"
TMP_DIR="$(mktemp -d)"
ARCHIVE_PATH=""

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

mkdir -p "$OUTPUT_DIR"

stream_extract_archive() {
  case "$SOURCE" in
    s3://*)
      case "$SOURCE" in
        *.tar.gz|*.tgz)
          echo "Streaming gzip-compressed tar from S3: $SOURCE"
          aws s3 cp "$SOURCE" - | tar -xzf - -C "$OUTPUT_DIR"
          ;;
        *.tar)
          echo "Streaming tar from S3: $SOURCE"
          aws s3 cp "$SOURCE" - | tar -xf - -C "$OUTPUT_DIR"
          ;;
        *)
          ARCHIVE_PATH="$TMP_DIR/$(basename "$SOURCE")"
          echo "Downloading from S3: $SOURCE"
          aws s3 cp "$SOURCE" "$ARCHIVE_PATH"
          extract_local_archive
          ;;
      esac
      ;;
    https://*)
      case "$SOURCE" in
        *.tar.gz|*.tgz)
          echo "Streaming gzip-compressed tar from HTTPS URL: $SOURCE"
          curl -L "$SOURCE" | tar -xzf - -C "$OUTPUT_DIR"
          ;;
        *.tar)
          echo "Streaming tar from HTTPS URL: $SOURCE"
          curl -L "$SOURCE" | tar -xf - -C "$OUTPUT_DIR"
          ;;
        *)
          ARCHIVE_PATH="$TMP_DIR/$(basename "$SOURCE")"
          echo "Downloading from HTTPS URL: $SOURCE"
          curl -L "$SOURCE" -o "$ARCHIVE_PATH"
          extract_local_archive
          ;;
      esac
      ;;
    *)
      echo "Unsupported source: $SOURCE"
      echo "Use an s3:// URI or an https:// URL."
      exit 1
      ;;
  esac
}

extract_local_archive() {
  case "$ARCHIVE_PATH" in
    *.tar.gz|*.tgz)
      echo "Extracting gzip-compressed tar archive"
      tar -xzf "$ARCHIVE_PATH" -C "$OUTPUT_DIR"
      ;;
    *.tar)
      echo "Extracting tar archive"
      tar -xf "$ARCHIVE_PATH" -C "$OUTPUT_DIR"
      ;;
    *.gz)
      echo "Decompressing gzip file"
      gunzip -c "$ARCHIVE_PATH" > "$OUTPUT_DIR/$(basename "${ARCHIVE_PATH%.gz}")"
      ;;
    *)
      echo "Unknown archive type: $ARCHIVE_PATH"
      echo "Expected .tar, .tar.gz, .tgz, or .gz"
      exit 1
      ;;
  esac
}

extract_nested_archives() {
  local found=1

  while IFS= read -r -d '' file; do
    found=0
    case "$file" in
      *.tar.gz|*.tgz)
        local target_dir="${file%.tar.gz}"
        target_dir="${target_dir%.tgz}"
        mkdir -p "$target_dir"
        echo "Extracting nested archive: $file -> $target_dir"
        tar -xzf "$file" -C "$target_dir"
        rm -f "$file"
        ;;
      *.tar)
        local target_dir="${file%.tar}"
        mkdir -p "$target_dir"
        echo "Extracting nested tar: $file -> $target_dir"
        tar -xf "$file" -C "$target_dir"
        rm -f "$file"
        ;;
      *.gz)
        echo "Decompressing nested gzip: $file"
        gunzip -f "$file"
        ;;
    esac
  done < <(find "$OUTPUT_DIR" -type f \( -name "*.tar.gz" -o -name "*.tgz" -o -name "*.tar" -o -name "*.gz" \) -print0)

  return $found
}

stream_extract_archive

while extract_nested_archives; do
  :
done

echo
echo "Extraction complete. Key CSV files found:"
find "$OUTPUT_DIR" -type f \( -name "patients.csv" -o -name "conditions.csv" -o -name "encounters.csv" -o -name "observations.csv" \) | sort
