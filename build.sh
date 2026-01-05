#!/usr/bin/env bash
# ./build.sh [component] [--tag tag] [--push] [-r registry] [-u username] [-p password] [--no-cache] [--use-nerdctl]
# leave registry empty if default registry [docker.io] used

set -e

# Initialize default values
COMPANY="hystax"
REGISTRY=""
LOGIN=""
PASSWORD=""
COMPONENT=""
INPUT_TAG=""
FLAGS=""
NO_CACHE=false
USE_NERDCTL=false
BUILD_TOOL="docker"
PUSH=false

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --tag) INPUT_TAG="$2"; shift ;;
        --push) PUSH=true ;;
        -r) REGISTRY="$2"; shift ;;
        -u) LOGIN="$2"; shift ;;
        -p) PASSWORD="$2"; shift ;;
        --no-cache) NO_CACHE=true ;;
        --use-nerdctl) USE_NERDCTL=true ;;
        *)
            # Set COMPONENT if not already set
            if [[ -z "$COMPONENT" ]]; then
                COMPONENT="$1"
            fi
            ;;
    esac
    shift
done

# Set build tool based on flag
if [[ "$USE_NERDCTL" == true ]]; then
    BUILD_TOOL="nerdctl"
fi

# Set --no-cache flag
if [[ "$NO_CACHE" == true ]]; then
    FLAGS="--no-cache"
fi

BUILD_TAG=${INPUT_TAG:-'local'}
FIND_CMD="find . -mindepth 2 -maxdepth 3 -print | grep Dockerfile | grep -vE '(test|.j2)'"
FIND_CMD="${FIND_CMD} | grep $COMPONENT/"

# Login to registry if push is enabled
if [[ "$PUSH" == true ]]; then
  if [[ -z "${LOGIN}" || -z "${PASSWORD}" ]]; then
    echo "Error: --push requires -u (username) and -p (password)"
    exit 1
  fi
  echo "$BUILD_TOOL login"
  $BUILD_TOOL login -u "${LOGIN}" -p "${PASSWORD}"
fi

push_image () {
   echo "Pushing $1:$2"
    if [ -z $3 ]; then
      $BUILD_TOOL tag "$1:$2" "$COMPANY/$1:$2"
      $BUILD_TOOL push "$COMPANY/$1:$2"
    else
      $BUILD_TOOL tag "$1:$2" "$3/$1:$2"
      $BUILD_TOOL push "$3/$1:$2"
    fi
}

build_and_push_component() {
    local DOCKERFILE=$1
    local COMPONENT_NAME=$2
    local BUILD_TAG=$3
    local FLAGS=$4
    local PUSH=$5
    local REGISTRY=$6
    local BUILD_TOOL=$7

    echo "[${COMPONENT_NAME}] Starting build..."
    if $BUILD_TOOL build $FLAGS -t ${COMPONENT_NAME}:${BUILD_TAG} -f ${DOCKERFILE} . --platform linux/amd64; then
        echo "[${COMPONENT_NAME}] Build successful"

        if [[ "$PUSH" == true ]]; then
            echo "[${COMPONENT_NAME}] Starting push..."
            if push_image $COMPONENT_NAME $BUILD_TAG $REGISTRY; then
                echo "[${COMPONENT_NAME}] Push successful"
                return 0
            else
                echo "[${COMPONENT_NAME}] Push failed"
                return 1
            fi
        fi
        return 0
    else
        echo "[${COMPONENT_NAME}] Build failed"
        return 1
    fi
}

export -f push_image
export -f build_and_push_component
export COMPANY
export BUILD_TOOL


declare -a PIDS
declare -a COMPONENTS

for DOCKERFILE in $(eval ${FIND_CMD} | xargs)
do
    COMPONENT_NAME=$(echo "${DOCKERFILE}" | awk -F '/' '{print $(NF-1)}')
    echo "Queuing build for ${COMPONENT_NAME}, build tag: ${BUILD_TAG}"
    build_and_push_component "$DOCKERFILE" "$COMPONENT_NAME" "$BUILD_TAG" "$FLAGS" "$PUSH" "$REGISTRY" "$BUILD_TOOL" &
    PIDS+=($!)
    COMPONENTS+=("$COMPONENT_NAME")
done

echo "=== All builds started, waiting for completion ==="
echo "Total components: ${#PIDS[@]}"

FAILED=false
declare -a FAILED_COMPONENTS

for i in "${!PIDS[@]}"; do
    PID=${PIDS[$i]}
    COMPONENT_NAME=${COMPONENTS[$i]}

    echo "Waiting for ${COMPONENT_NAME} (PID: $PID)..."

    if wait $PID; then
        echo "✓ ${COMPONENT_NAME} completed successfully"
    else
        EXIT_CODE=$?
        echo "✗ ${COMPONENT_NAME} failed with exit code $EXIT_CODE"
        FAILED=true
        FAILED_COMPONENTS+=("$COMPONENT_NAME")
    fi
done

echo ""
echo "=== Build Summary ==="
echo "Total components: ${#PIDS[@]}"

if [[ "$FAILED" == true ]]; then
    echo "Failed components: ${#FAILED_COMPONENTS[@]}"
    echo "Failed: ${FAILED_COMPONENTS[*]}"
    echo ""
    echo "❌ Build failed!"
    exit 1
else
    echo "✓ All components built successfully!"
    exit 0
fi
