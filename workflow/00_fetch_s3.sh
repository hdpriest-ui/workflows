#!/usr/bin/env bash
# 00_fetch_s3.sh: fetch a single S3 artifact into the run directory.
# Invoked as the fetch_demo_data step; each invocation downloads exactly one artifact.
# Tarballs (.tgz, .tar.gz) are extracted into the run directory.
# Non-tarball files are downloaded to dest_path (relative to run_dir) from the manifest.
# The run directory must already exist (created by the preceding stage_inputs step).
# S3 URLs and path keys come from the workflow manifest; run dir is passed as an argument.
#
# Requires: yq (mikefarah/yq), aws CLI
#
# Options (see --help): --repo-root (required); --manifest optional,
# defaults to <repo-root>/workflow/workflow_manifest.yaml.

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: 00_fetch_s3.sh [OPTIONS]

Fetch demo data from S3 into the run directory. S3 URLs and path keys are
read from the workflow manifest. The run directory must already exist (created
by the stage_inputs step).

Required:
  --repo-root PATH      Repo root (workflows directory). Script changes to this directory.
  --artifact KEY        Key in manifest .s3 section identifying the tarball to download (e.g. artifact_02).

Run directory (one of):
  --run-dir PATH        Run directory (absolute, or relative to --repo-root).
  --config PATH         User YAML config file; script reads run_dir from it (use with --invocation-cwd).

Optional:
  --manifest PATH       Path to workflow_manifest.yaml (default: <repo-root>/workflow/workflow_manifest.yaml).
  --invocation-cwd PATH Required when using --config with a relative run_dir. Paths reported relative to this.
  -h, --help            Print this help and exit.
EOF
}

RUN_DIR=""
CONFIG_FILE=""
REPO_ROOT=""
MANIFEST=""
INVOCATION_CWD=""
ARTIFACT_KEY=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-dir)        [[ $# -lt 2 ]] && { echo "00_fetch_s3: --run-dir requires PATH." >&2; usage >&2; exit 1; }; RUN_DIR="$2"; shift 2 ;;
    --config)         [[ $# -lt 2 ]] && { echo "00_fetch_s3: --config requires PATH." >&2; usage >&2; exit 1; }; CONFIG_FILE="$2"; shift 2 ;;
    --repo-root)      [[ $# -lt 2 ]] && { echo "00_fetch_s3: --repo-root requires PATH." >&2; usage >&2; exit 1; }; REPO_ROOT="$2"; shift 2 ;;
    --manifest)       [[ $# -lt 2 ]] && { echo "00_fetch_s3: --manifest requires PATH." >&2; usage >&2; exit 1; }; MANIFEST="$2"; shift 2 ;;
    --invocation-cwd) [[ $# -lt 2 ]] && { echo "00_fetch_s3: --invocation-cwd requires PATH." >&2; usage >&2; exit 1; }; INVOCATION_CWD="$2"; shift 2 ;;
    --artifact)       [[ $# -lt 2 ]] && { echo "00_fetch_s3: --artifact requires KEY." >&2; usage >&2; exit 1; }; ARTIFACT_KEY="$2"; shift 2 ;;
    -h|--help)        usage; exit 0 ;;
    *)                echo "00_fetch_s3: Unknown option: $1" >&2; usage >&2; exit 1 ;;
  esac
done

if [[ -z "$REPO_ROOT" ]]; then echo "00_fetch_s3: --repo-root is required." >&2; usage >&2; exit 1; fi
if [[ -z "$ARTIFACT_KEY" ]]; then echo "00_fetch_s3: --artifact is required." >&2; usage >&2; exit 1; fi
if [[ -z "$MANIFEST" ]]; then
  MANIFEST="${REPO_ROOT}/workflow/workflow_manifest.yaml"
fi

# Run directory: from --run-dir or from config file
if [[ -n "$CONFIG_FILE" ]]; then
  if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "00_fetch_s3: Config file not found: $CONFIG_FILE" >&2
    exit 1
  fi
  RUN_DIR=$(yq eval '.run_dir' "$CONFIG_FILE") || { echo "00_fetch_s3: yq failed to read .run_dir from config: $CONFIG_FILE" >&2; exit 1; }
  if [[ -z "$RUN_DIR" || "$RUN_DIR" == "null" ]]; then
    echo "00_fetch_s3: run_dir not found or empty in config (expected .run_dir): $CONFIG_FILE" >&2
    exit 1
  fi
  if [[ "$RUN_DIR" != /* ]]; then
    if [[ -z "$INVOCATION_CWD" ]]; then
      echo "00_fetch_s3: --invocation-cwd is required when run_dir in config is relative." >&2
      exit 1
    fi
    RUN_DIR="${INVOCATION_CWD}/${RUN_DIR}"
  fi
elif [[ -z "$RUN_DIR" ]]; then
  echo "00_fetch_s3: Provide --run-dir or --config (with run_dir in the config file)." >&2
  usage >&2
  exit 1
fi

# Show path for user: relative to INVOCATION_CWD if under it, else absolute
report_path() {
  local abs_path="$1"
  if [[ -n "$INVOCATION_CWD" && "$abs_path" == "$INVOCATION_CWD"/* ]]; then
    echo "${abs_path#"$INVOCATION_CWD"/}"
  else
    echo "$abs_path"
  fi
}

if [[ ! -f "$MANIFEST" ]]; then
  echo "00_fetch_s3: Manifest not found: $MANIFEST" >&2
  exit 1
fi

if ! command -v yq &>/dev/null; then
  echo "00_fetch_s3: yq is required to read the manifest." >&2
  exit 1
fi

cd "$REPO_ROOT"

# --- Read from manifest (endpoint, bucket, and per-resource key_prefix + filename) ---
s3_endpoint=$(yq eval '.s3.endpoint_url' "$MANIFEST")
s3_bucket=$(yq eval '.s3.bucket' "$MANIFEST")

# Build S3 key from key_prefix + filename (key_prefix may be empty or null from yq)
s3_key() {
  local prefix="$1"
  local name="$2"
  [[ "$prefix" == "null" || -z "$prefix" ]] && prefix=""
  if [[ -n "$prefix" ]]; then
    echo "${prefix}/${name}"
  else
    echo "$name"
  fi
}

# Artifact: bucket + key from the manifest s3 entry named by --artifact
artifact_key_prefix=$(yq eval '.s3["'"$ARTIFACT_KEY"'"].key_prefix' "$MANIFEST")
artifact_filename=$(yq eval '.s3["'"$ARTIFACT_KEY"'"].filename' "$MANIFEST")
if [[ -z "$artifact_filename" || "$artifact_filename" == "null" ]]; then
  echo "00_fetch_s3: s3 artifact key '$ARTIFACT_KEY' not found in manifest" >&2
  exit 1
fi
artifact_dest_path=$(yq eval '.s3["'"$ARTIFACT_KEY"'"].dest_path' "$MANIFEST")
artifact_s3_key=$(s3_key "$artifact_key_prefix" "$artifact_filename")
artifact_s3_uri="s3://${s3_bucket}/${artifact_s3_key}"

# --- Verify run directory exists (must be created by the preceding stage_inputs step) ---
RUN_DIR_ABS=$(if [[ "$RUN_DIR" = /* ]]; then echo "$RUN_DIR"; else echo "$REPO_ROOT/$RUN_DIR"; fi)
if [[ ! -d "$RUN_DIR_ABS" ]]; then
  echo "00_fetch_s3: Run directory does not exist: $RUN_DIR_ABS" >&2
  echo "00_fetch_s3: The stage_inputs step must run before fetch_demo_data." >&2
  exit 1
fi
RUN_DIR_ABS=$(cd "$RUN_DIR_ABS" && pwd)

# --- Download artifact ---
if [[ "$artifact_filename" == *.tgz || "$artifact_filename" == *.tar.gz ]]; then
  artifact_local="${RUN_DIR_ABS}/${artifact_filename}"
  if [[ -f "$artifact_local" ]]; then
    echo "00_fetch_s3: Artifact already present, skipping download and extraction: $(report_path "$artifact_local")"
  else
    echo "00_fetch_s3: Downloading artifact from S3 into run directory"
    echo "00_fetch_s3: Saving to: $(report_path "$artifact_local")"
    (cd "$RUN_DIR_ABS" && aws s3 cp --endpoint-url "$s3_endpoint" "$artifact_s3_uri" "$artifact_filename")
    echo "00_fetch_s3: Extracting artifact into run directory"
    tar -xzf "$artifact_local" -C "$RUN_DIR_ABS"
  fi
else
  if [[ -z "$artifact_dest_path" || "$artifact_dest_path" == "null" ]]; then
    artifact_dest_path="$artifact_filename"
  fi
  artifact_local="${RUN_DIR_ABS}/${artifact_dest_path}"
  if [[ -f "$artifact_local" ]]; then
    echo "00_fetch_s3: Already present, skipping: $(report_path "$artifact_local")"
  else
    mkdir -p "$(dirname "$artifact_local")"
    echo "00_fetch_s3: Downloading from S3"
    echo "00_fetch_s3: Saving to: $(report_path "$artifact_local")"
    aws s3 cp --endpoint-url "$s3_endpoint" "$artifact_s3_uri" "$artifact_local"
  fi
fi

echo "00_fetch_s3: Done."
