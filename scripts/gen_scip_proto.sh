#!/usr/bin/env bash
# Regenerate vendored SCIP protobuf bindings.
# scip.proto is vendored from https://github.com/sourcegraph/scip (Apache-2.0).
set -euo pipefail
cd "$(dirname "$0")/.."

SCIP_REF="${SCIP_REF:-v0.9.0}"   # pinned release tag of sourcegraph/scip
DEST=src/codegraph/resolvers/scip

curl -fsSL "https://raw.githubusercontent.com/sourcegraph/scip/${SCIP_REF}/scip.proto" \
  -o "${DEST}/scip.proto"

uv run python -m grpc_tools.protoc \
  -I "${DEST}" \
  --python_out="${DEST}" \
  "${DEST}/scip.proto"

echo "regenerated ${DEST}/scip_pb2.py from sourcegraph/scip@${SCIP_REF}"
