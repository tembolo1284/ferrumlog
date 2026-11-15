#!/usr/bin/env bash
#
# One entry point for building, checking, and running ferrumlog.
# Run ./build.sh help for the list of commands.

set -euo pipefail

# Work from the repository root no matter where the script is called from.
cd "$(dirname "${BASH_SOURCE[0]}")"

IMAGE="${FERRUMLOG_IMAGE:-ferrumlog:local}"
DATA_DIR="${FERRUMLOG_DATA_DIR:-./data}"

usage() {
    cat <<'EOF'
Usage: ./build.sh <command> [args]

  build        Build every crate in release mode
  test         Run every test in the workspace
  lint         Check formatting and run clippy with warnings as errors
  check        lint + test. This is what CI runs
  fmt          Format the code in place
  run [args]   Start a node on 127.0.0.1:7001 with its data in ./data.
               Extra args go to fl-server, for example: ./build.sh run --node-id 2
  demo         Start a throwaway node, run a short fl session against it, stop it
  docker       Build the container image (tag: ferrumlog:local)
  clean        Remove build output
  help         Show this text

Environment:
  FERRUMLOG_DATA_DIR   Data directory used by "run" (default ./data)
  FERRUMLOG_IMAGE      Image tag used by "docker" (default ferrumlog:local)
  FERRUMLOG_DEMO_PORT  Port used by "demo" (default 17001)
EOF
}

say() {
    printf '\n==> %s\n' "$*"
}

build() {
    say "building (release)"
    cargo build --release --workspace
}

run_tests() {
    say "running tests"
    cargo test --workspace
}

lint() {
    say "checking formatting"
    cargo fmt --all -- --check
    say "running clippy"
    cargo clippy --workspace --all-targets -- -D warnings
}

fmt() {
    cargo fmt --all
}

run_node() {
    cargo build --release -p fl-server
    say "starting a node, data in ${DATA_DIR} (Ctrl-C to stop)"
    exec target/release/fl-server --data-dir "${DATA_DIR}" "$@"
}

demo() {
    cargo build --release -p fl-server -p fl-cli

    local dir port pid
    dir="$(mktemp -d)"
    port="${FERRUMLOG_DEMO_PORT:-17001}"
    target/release/fl-server --data-dir "${dir}" --listen "127.0.0.1:${port}" --no-metrics \
        >"${dir}/server.log" 2>&1 &
    pid=$!
    # Stop the node and remove its data however the demo ends. The values
    # are expanded now, because these locals are gone by the time the trap
    # runs.
    # shellcheck disable=SC2064
    trap "kill ${pid} 2>/dev/null || true; wait ${pid} 2>/dev/null || true; rm -rf '${dir}'" EXIT
    # Turn the signals that would kill the script into a normal exit, so
    # the cleanup above also runs after Ctrl-C or a closed pipe.
    trap 'exit 130' INT TERM HUP PIPE

    export FERRUMLOG_ENDPOINTS="127.0.0.1:${port}"
    local fl=target/release/fl

    # Wait for the node to answer, for up to five seconds. If the process
    # has already exited, for example because the port is taken, stop at
    # once. Otherwise the demo could end up talking to some other node that
    # happens to be listening there.
    local tries=0
    while true; do
        if ! kill -0 "${pid}" 2>/dev/null; then
            echo "the node exited during startup, its log follows:" >&2
            cat "${dir}/server.log" >&2
            exit 1
        fi
        if "${fl}" --timeout 1 status >/dev/null 2>&1; then
            break
        fi
        tries=$((tries + 1))
        if [ "${tries}" -ge 50 ]; then
            echo "the node did not answer, its log follows:" >&2
            cat "${dir}/server.log" >&2
            exit 1
        fi
        sleep 0.1
    done

    # Print each command before running it, the way a terminal would show
    # it, with quotes around any argument that contains a space.
    show() {
        local arg
        printf '\n$ fl'
        for arg in "$@"; do
            case "${arg}" in
                *' '*) printf ' "%s"' "${arg}" ;;
                *) printf ' %s' "${arg}" ;;
            esac
        done
        printf '\n'
        "${fl}" "$@"
    }

    show create-topic quotes
    show produce quotes "UST10Y 99.5" "UST2Y 100.1"
    printf '\n$ printf '"'"'a\\nb\\nc\\n'"'"' | fl produce quotes --key batch\n'
    printf 'a\nb\nc\n' | "${fl}" produce quotes --key batch
    show consume quotes
    show consume quotes --follow --from 3 -n 2
    show topics
    show status

    say "demo finished, stopping the node"
}

docker_image() {
    say "building image ${IMAGE}"
    docker build -f deploy/docker/Dockerfile -t "${IMAGE}" .
}

clean() {
    cargo clean
}

command="${1:-help}"
shift || true

case "${command}" in
    build) build ;;
    test) run_tests ;;
    lint) lint ;;
    check)
        lint
        run_tests
        ;;
    fmt) fmt ;;
    run) run_node "$@" ;;
    demo) demo ;;
    docker) docker_image ;;
    clean) clean ;;
    help | -h | --help) usage ;;
    *)
        echo "unknown command: ${command}" >&2
        echo >&2
        usage >&2
        exit 2
        ;;
esac
