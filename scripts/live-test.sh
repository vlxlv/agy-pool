#!/usr/bin/env bash
#
# scripts/live-test.sh: Live integration and scheduler smoke-test harness for agy-pool.
#
# Validates real agy-pool gateway routing, daemon operations, and scheduling strategies
# (round_robin, least_used, max_quota) against live accounts and upstream Google API.
#
# Zero external runtime dependencies: Bash + Python 3.8+ standard library only.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

BIN_AGY_POOL="${REPO_ROOT}/bin/agy-pool"
BIN_AGY_RAW="${REPO_ROOT}/bin/agy-raw"
LIVE_TEST_PY="${SCRIPT_DIR}/live_test.py"
PYTHON="${PYTHON:-python3}"
LOG_FILE="${HOME}/.gemini/agy-pool.log"

# Default settings
COMMAND=""
RUNS=0
TIMEOUT=30
KEEP_ARTIFACTS=false
JSON_OUTPUT=false
AUTO_CONFIRM=false
TMP_DIR=""
ORIG_STRATEGY=""

# Terminal colors (if supported)
if [ -t 1 ]; then
    CLR_RESET="\033[0m"
    CLR_BOLD="\033[1m"
    CLR_GREEN="\033[32m"
    CLR_YELLOW="\033[33m"
    CLR_RED="\033[31m"
    CLR_CYAN="\033[36m"
else
    CLR_RESET=""
    CLR_BOLD=""
    CLR_GREEN=""
    CLR_YELLOW=""
    CLR_RED=""
    CLR_CYAN=""
fi

usage() {
    cat <<EOF
Usage: $(basename "$0") [COMMAND] [OPTIONS]

Live integration and scheduler smoke-test harness for agy-pool.

Commands (Safe / Non-quota-consuming):
  doctor            Run environment diagnostics and verify binaries (default)
  port              Verify gateway port resolution and connectivity
  status            Inspect daemon status and display parsed account pool

Commands (Live Quota-consuming):
  gateway           Run single generation request through agy-pool gateway
  round-robin       Verify sequential cyclic rotation under round_robin strategy
  least-used        Verify load distribution to lowest-hit accounts under least_used
  max-quota         Verify highest-quota priority under max_quota strategy
  scheduler         Run scheduler test across eligible pool accounts
  all               Run doctor, gateway single test, and scheduler validation

Options:
  -n, --runs <N>       Number of requests to execute (default depends on command)
  -t, --timeout <sec>  Per-request timeout in seconds (default: 30)
  -k, --keep-artifacts Preserve temporary log and artifact directory in /tmp
  -j, --json           Output results in JSON format
  -y, --yes            Skip live quota consumption warning prompt
  -h, --help           Show this help message

Examples:
  $(basename "$0") doctor
  $(basename "$0") gateway --runs 1
  $(basename "$0") round-robin --runs 4 -y
  $(basename "$0") all --timeout 45
EOF
}

# Cleanup and restoration trap
cleanup() {
    local exit_code=$?
    if [ -n "$ORIG_STRATEGY" ]; then
        echo -e "${CLR_YELLOW}[CLEANUP] Restoring load balancing strategy to '${ORIG_STRATEGY}'...${CLR_RESET}" >&2
        "$PYTHON" "$BIN_AGY_POOL" strategy "$ORIG_STRATEGY" >/dev/null 2>&1 || true
    fi
    if [ "$KEEP_ARTIFACTS" = "false" ] && [ -n "${TMP_DIR:-}" ] && [ -d "$TMP_DIR" ]; then
        rm -rf "$TMP_DIR"
    elif [ -n "${TMP_DIR:-}" ] && [ -d "$TMP_DIR" ]; then
        echo -e "${CLR_CYAN}[INFO] Artifacts preserved at: ${TMP_DIR}${CLR_RESET}" >&2
    fi
    exit "$exit_code"
}
trap cleanup EXIT INT TERM

# Run command with timeout using timeout binary or Python fallback
run_with_timeout() {
    local to="$1"
    shift
    if command -v timeout >/dev/null 2>&1; then
        timeout "$to" "$@"
    else
        "$PYTHON" -c "
import subprocess, sys
try:
    p = subprocess.run(sys.argv[2:], timeout=float(sys.argv[1]))
    sys.exit(p.returncode)
except subprocess.TimeoutExpired:
    sys.stderr.write('Command timed out after ' + sys.argv[1] + 's\n')
    sys.exit(124)
" "$to" "$@"
    fi
}

# Self-check dependencies and executables
check_environment() {
    if ! command -v "$PYTHON" >/dev/null 2>&1; then
        echo -e "${CLR_RED}[Error] Python 3 executable not found (${PYTHON}).${CLR_RESET}" >&2
        exit 1
    fi

    # Verify python version >= 3.8
    "$PYTHON" -c "
import sys
if sys.version_info < (3, 8):
    sys.stderr.write('[Error] Python 3.8 or higher is required.\n')
    sys.exit(1)
" || exit 1

    if [ ! -f "$BIN_AGY_POOL" ]; then
        echo -e "${CLR_RED}[Error] bin/agy-pool not found at ${BIN_AGY_POOL}.${CLR_RESET}" >&2
        exit 1
    fi

    if [ ! -f "$LIVE_TEST_PY" ]; then
        echo -e "${CLR_RED}[Error] scripts/live_test.py not found at ${LIVE_TEST_PY}.${CLR_RESET}" >&2
        exit 1
    fi
}

get_current_strategy() {
    local strat
    strat="$("$PYTHON" -c "
import json, os
pool_path = os.path.expanduser('~/.gemini/agy-pool-accounts.json')
if os.path.exists(pool_path):
    try:
        with open(pool_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            print(data.get('strategy', 'max_quota'))
    except Exception:
        print('max_quota')
else:
    print('max_quota')
")"
    echo "$strat"
}

get_ready_accounts_count() {
    local acc_file="$1"
    "$PYTHON" -c "
import json, sys
accounts = json.load(open(sys.argv[1]))
eligible = [a for a in accounts if a.get('is_eligible', True)]
print(len(eligible))
" "$acc_file"
}

warn_quota_usage() {
    local num_runs="$1"
    if [ "$AUTO_CONFIRM" = "true" ]; then
        return 0
    fi
    echo -e "${CLR_YELLOW}--------------------------------------------------------------------${CLR_RESET}"
    echo -e "${CLR_BOLD}WARNING: Live test will send ${num_runs} generation request(s) to upstream Gemini API.${CLR_RESET}"
    echo -e "This consumes real account quota from your agy-pool."
    echo -e "${CLR_YELLOW}--------------------------------------------------------------------${CLR_RESET}"
    read -r -p "Continue with live quota consumption? [y/N] " response
    case "$response" in
        [yY][eE][sS]|[yY])
            ;;
        *)
            echo "Aborted by user."
            exit 0
            ;;
    esac
}

# Subcommand: doctor
cmd_doctor() {
    echo -e "${CLR_BOLD}${CLR_CYAN}=== Running agy-pool Environment Doctor ===${CLR_RESET}"
    "$PYTHON" "$BIN_AGY_POOL" doctor
}

# Subcommand: port
cmd_port() {
    echo -e "${CLR_BOLD}${CLR_CYAN}=== Testing Gateway Port Resolution ===${CLR_RESET}"
    local port
    port="$("$PYTHON" "$LIVE_TEST_PY" get-port)"
    echo -e "Resolved Gateway Port: ${CLR_BOLD}${port}${CLR_RESET}"
    
    local listening
    listening="$("$PYTHON" -c "
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(1.0)
try:
    s.connect(('127.0.0.1', ${port}))
    s.close()
    print('LISTEN')
except Exception:
    print('CLOSED')
")"
    if [ "$listening" = "LISTEN" ]; then
        echo -e "Gateway Socket (127.0.0.1:${port}): ${CLR_GREEN}ACTIVE (Listening)${CLR_RESET}"
    else
        echo -e "Gateway Socket (127.0.0.1:${port}): ${CLR_YELLOW}INACTIVE (Daemon stopped)${CLR_RESET}"
    fi
}

# Subcommand: status
cmd_status() {
    echo -e "${CLR_BOLD}${CLR_CYAN}=== Pool & Gateway Status Inspection ===${CLR_RESET}"
    "$PYTHON" "$BIN_AGY_POOL" status
}

# Execute a single prompt through agy-pool gateway
# Arguments: $1: prompt, $2: run_idx, $3: output_dir
run_gateway_prompt() {
    local prompt="$1"
    local run_idx="$2"
    local out_dir="$3"

    local stdout_file="${out_dir}/run_${run_idx}.stdout"
    local stderr_file="${out_dir}/run_${run_idx}.stderr"

    local start_ts
    start_ts="$("$PYTHON" -c "import time; print(f'{time.time():.3f}')")"

    set +e
    run_with_timeout "$TIMEOUT" "$BIN_AGY_POOL" run -p "$prompt" >"$stdout_file" 2>"$stderr_file"
    local exit_code=$?
    set -e

    local end_ts
    end_ts="$("$PYTHON" -c "import time; print(f'{time.time():.3f}')")"

    local duration
    duration="$("$PYTHON" -c "print(f'{float(${end_ts}) - float(${start_ts}):.2f}')")"

    echo "$exit_code" > "${out_dir}/run_${run_idx}.exit"
    echo "$duration" > "${out_dir}/run_${run_idx}.duration"

    return "$exit_code"
}

# Run live strategy test
# Arguments: $1: strategy_name, $2: runs
run_strategy_test() {
    local target_strategy="$1"
    local test_runs="$2"

    TMP_DIR="$(mktemp -d "/tmp/agy-live-test.XXXXXX")"

    echo -e "\n${CLR_BOLD}${CLR_CYAN}=== Live Scheduler Validation: ${target_strategy} ===${CLR_RESET}"
    echo "Workspace: ${TMP_DIR}"

    # Snapshot accounts before test
    "$PYTHON" "$BIN_AGY_POOL" list > "${TMP_DIR}/list_before.txt" 2>&1 || true
    "$PYTHON" "$LIVE_TEST_PY" parse-accounts "${TMP_DIR}/list_before.txt" > "${TMP_DIR}/accounts_before.json"

    local total_acc
    total_acc="$("$PYTHON" -c "import json; print(len(json.load(open('${TMP_DIR}/accounts_before.json'))))")"
    if [ "$total_acc" -eq 0 ]; then
        echo -e "${CLR_RED}[Error] Account pool is empty (~/.gemini/agy-pool-accounts.json).${CLR_RESET}" >&2
        echo "Add accounts with 'agy-pool login' or 'agy-pool import-current' before running live tests." >&2
        exit 1
    fi

    local ready_acc
    ready_acc="$(get_ready_accounts_count "${TMP_DIR}/accounts_before.json")"
    echo "Account pool: ${total_acc} total, ${ready_acc} ready/eligible."

    if [ "$ready_acc" -lt 1 ]; then
        echo -e "${CLR_RED}[Error] No ready/eligible accounts available in pool for generation.${CLR_RESET}" >&2
        exit 1
    fi

    if [ "$target_strategy" = "round_robin" ] && [ "$ready_acc" -lt 2 ]; then
        echo -e "${CLR_YELLOW}[WARN] Only 1 ready account available. Sequential rotation across different accounts cannot be observed.${CLR_RESET}"
    fi

    # Determine runs if not explicitly set
    if [ "$test_runs" -le 0 ]; then
        if [ "$target_strategy" = "round_robin" ]; then
            test_runs="$ready_acc"
            [ "$test_runs" -lt 2 ] && test_runs=2
            [ "$test_runs" -gt 4 ] && test_runs=4
        else
            test_runs=3
        fi
    fi

    warn_quota_usage "$test_runs"

    # Save original strategy and switch to target
    ORIG_STRATEGY="$(get_current_strategy)"
    if [ "$ORIG_STRATEGY" != "$target_strategy" ]; then
        echo -e "Setting pool strategy to: ${CLR_BOLD}${target_strategy}${CLR_RESET} (was: ${ORIG_STRATEGY})"
        "$PYTHON" "$BIN_AGY_POOL" strategy "$target_strategy" >/dev/null
    fi

    # Record log offset before requests
    local start_offset
    start_offset="$("$PYTHON" "$LIVE_TEST_PY" log-offset "$LOG_FILE")"
    echo "Gateway log starting offset: ${start_offset} bytes"

    # Execute requests
    echo -e "\nExecuting ${test_runs} generation request(s)..."
    local successful_runs=0
    for ((i=1; i<=test_runs; i++)); do
        printf "  Request [%d/%d] ... " "$i" "$test_runs"
        local exit_code=0
        if run_gateway_prompt "Reply with exactly one word: PONG" "$i" "$TMP_DIR"; then
            local dur
            dur="$(cat "${TMP_DIR}/run_${i}.duration")"
            local out_sample
            out_sample="$(head -n 2 "${TMP_DIR}/run_${i}.stdout" | tr '\n' ' ' | cut -c1-40)"
            echo -e "${CLR_GREEN}OK${CLR_RESET} (${dur}s) -> ${out_sample}..."
            successful_runs=$((successful_runs + 1))
        else
            exit_code=$?
            local dur
            dur="$(cat "${TMP_DIR}/run_${i}.duration")"
            if [ "$exit_code" -eq 124 ]; then
                echo -e "${CLR_RED}TIMEOUT${CLR_RESET} (${TIMEOUT}s limit reached)"
            else
                echo -e "${CLR_RED}FAIL${CLR_RESET} (exit code ${exit_code})"
                if [ -f "${TMP_DIR}/run_${i}.stderr" ]; then
                    head -n 3 "${TMP_DIR}/run_${i}.stderr" | sed 's/^/    /'
                fi
            fi
        fi
        sleep 1
    done

    # Snapshot accounts after test
    "$PYTHON" "$BIN_AGY_POOL" list > "${TMP_DIR}/list_after.txt" 2>&1 || true
    "$PYTHON" "$LIVE_TEST_PY" parse-accounts "${TMP_DIR}/list_after.txt" > "${TMP_DIR}/accounts_after.json"

    # Extract log delta
    "$PYTHON" "$LIVE_TEST_PY" parse-log-delta "$LOG_FILE" "$start_offset" > "${TMP_DIR}/log_delta.json"

    # Analyze hits delta
    "$PYTHON" "$LIVE_TEST_PY" hits-delta \
        --before "${TMP_DIR}/accounts_before.json" \
        --after "${TMP_DIR}/accounts_after.json" > "${TMP_DIR}/hits_delta.json"

    # Extract counts and metrics from log delta
    local proxy_events_count
    proxy_events_count="$("$PYTHON" -c "import json; d=json.load(open('${TMP_DIR}/log_delta.json')); print(d.get('total_proxy_events', len(d.get('gateway_proxy_events', []))))")"
    local gen_attempts_count
    gen_attempts_count="$("$PYTHON" -c "import json; d=json.load(open('${TMP_DIR}/log_delta.json')); print(d.get('generation_attempts_count', len(d.get('generation_attempts', []))))")"
    local gen_dispatches_count
    gen_dispatches_count="$("$PYTHON" -c "import json; d=json.load(open('${TMP_DIR}/log_delta.json')); print(d.get('successful_dispatches_count', len(d.get('generation_dispatches', d.get('dispatches', [])))))")"
    local aux_events_count
    aux_events_count="$("$PYTHON" -c "import json; d=json.load(open('${TMP_DIR}/log_delta.json')); print(d.get('auxiliary_events_count', len(d.get('auxiliary_events', []))))")"
    local failovers_count
    failovers_count="$("$PYTHON" -c "import json; d=json.load(open('${TMP_DIR}/log_delta.json')); print(d.get('failovers_count', len(d.get('failovers', []))))")"

    # Extract genuine generation dispatches and failovers
    "$PYTHON" -c "
import json
delta = json.load(open('${TMP_DIR}/log_delta.json'))
print(json.dumps(delta.get('generation_dispatches', delta.get('dispatches', []))))
" > "${TMP_DIR}/dispatches.json"

    "$PYTHON" -c "
import json
delta = json.load(open('${TMP_DIR}/log_delta.json'))
print(json.dumps(delta.get('failovers', [])))
" > "${TMP_DIR}/failovers.json"

    echo -e "\n${CLR_BOLD}Results Summary:${CLR_RESET}"
    echo "  • Successful requests: ${successful_runs}/${test_runs}"
    echo "  • Gateway proxy events: ${proxy_events_count}"
    echo "  • Generation attempts: ${gen_attempts_count}"
    echo "  • Successful generation dispatches: ${gen_dispatches_count}"
    echo "  • Auxiliary gateway events: ${aux_events_count}"

    if [ "$failovers_count" -gt 0 ]; then
        local failover_summary
        failover_summary="$("$PYTHON" -c "
import json
delta = json.load(open('${TMP_DIR}/log_delta.json'))
fails = delta.get('failovers', [])
summary = '; '.join(f\"{f.get('account')}: {f.get('reason', '')}\" for f in fails)
print(summary)
")"
        echo -e "  • ${CLR_YELLOW}Failovers recorded: ${failovers_count} (${failover_summary})${CLR_RESET}"
    fi

    # For scheduler tests print the account generation sequence when available
    local gen_seq_display
    gen_seq_display="$("$PYTHON" -c "
import json
dispatches = json.load(open('${TMP_DIR}/dispatches.json'))
if dispatches:
    print(' -> '.join(dispatches))
else:
    print('')
")"
    if [ -n "$gen_seq_display" ]; then
        echo "  • Account generation sequence: ${gen_seq_display}"
    fi

    # Verify scheduler
    local verify_res=""
    local verify_passed=false
    if [ "$gen_dispatches_count" -gt 0 ]; then
        set +e
        verify_res="$("$PYTHON" "$LIVE_TEST_PY" verify-scheduler \
            --strategy "$target_strategy" \
            --dispatches "${TMP_DIR}/dispatches.json" \
            --accounts "${TMP_DIR}/accounts_before.json")"
        if [ $? -eq 0 ]; then
            verify_passed=true
        fi
        set -e
    else
        verify_res='{"passed": false, "reason": "No successful generation dispatches recorded in gateway proxy log"}'
    fi

    # Check Hits delta
    local zero_hits_delta
    zero_hits_delta="$("$PYTHON" -c "import json; print(json.load(open('${TMP_DIR}/hits_delta.json')).get('zero_delta', False))")"
    local total_hits_delta
    total_hits_delta="$("$PYTHON" -c "import json; print(json.load(open('${TMP_DIR}/hits_delta.json')).get('total_delta', 0))")"
    local gcd_val
    gcd_val="$("$PYTHON" -c "import json; print(json.load(open('${TMP_DIR}/hits_delta.json')).get('gcd', 1))")"

    echo "  • Hits delta: total +${total_hits_delta} (GCD unit: ${gcd_val})"
    echo "  • Per-account Hits:"
    "$PYTHON" -c "
import json
h = json.load(open('${TMP_DIR}/hits_delta.json'))
per_acc = h.get('per_account', [])
if per_acc:
    print(f'    {\"ACCOUNT\":<35} {\"BEFORE\":>7} {\"AFTER\":>7} {\"DELTA\":>7}')
    for item in per_acc:
        acc = item['account']
        b = str(item['before'])
        a = str(item['after'])
        d = f\"+{item['delta']}\" if item['delta'] >= 0 else str(item['delta'])
        print(f'    {acc:<35} {b:>7} {a:>7} {d:>7}')
"

    # Detect concurrent activity (scoped to generation traffic)
    local conc_res
    conc_res="$("$PYTHON" "$LIVE_TEST_PY" detect-concurrent \
        --expected "$test_runs" \
        --dispatches "${TMP_DIR}/dispatches.json" \
        --hits-delta "${TMP_DIR}/hits_delta.json" \
        --failovers "${TMP_DIR}/failovers.json")"
    local traffic_status
    traffic_status="$("$PYTHON" -c "import json; print(json.loads('''$conc_res''').get('status', 'CLEAN'))")"
    local traffic_msg
    traffic_msg="$("$PYTHON" -c "import json; print(json.loads('''$conc_res''').get('message', ''))")"

    if [ "$traffic_status" = "CONCURRENT" ]; then
        echo -e "  • ${CLR_RED}Traffic check: CONCURRENT (external pool traffic detected)${CLR_RESET}"
    elif [ "$traffic_status" = "INCONCLUSIVE" ]; then
        echo -e "  • ${CLR_YELLOW}Traffic check: INCONCLUSIVE (${traffic_msg})${CLR_RESET}"
    else
        echo -e "  • ${CLR_GREEN}Traffic check: CLEAN (isolated test traffic)${CLR_RESET}"
    fi

    if [ "$successful_runs" -gt 0 ] && [ "$zero_hits_delta" = "True" ]; then
        echo -e "\n${CLR_RED}✖ SCHEDULER TEST FAILED: Zero Hits delta observed despite successful requests.${CLR_RESET}" >&2
        verify_passed=false
    elif [ "$traffic_status" = "CONCURRENT" ]; then
        echo -e "\n${CLR_YELLOW}⚠ SCHEDULER TEST INCONCLUSIVE: Confirmed concurrent external pool traffic was detected.${CLR_RESET}" >&2
        verify_passed=false
    elif [ "$traffic_status" = "INCONCLUSIVE" ]; then
        echo -e "\n${CLR_YELLOW}⚠ SCHEDULER TEST INCONCLUSIVE: ${traffic_msg}${CLR_RESET}"
        verify_passed=false
    elif [ "$verify_passed" = "true" ]; then
        local detail
        detail="$("$PYTHON" -c "import json; print(json.loads('''$verify_res''').get('details', 'OK'))")"
        echo -e "\n${CLR_GREEN}✓ SCHEDULER TEST PASSED: ${detail}${CLR_RESET}"
    else
        local reason
        reason="$("$PYTHON" -c "import json; print(json.loads('''$verify_res''').get('reason', 'Verification failed'))")"
        echo -e "\n${CLR_RED}✖ SCHEDULER TEST FAILED: ${reason}${CLR_RESET}" >&2
    fi

    # Output JSON if requested
    if [ "$JSON_OUTPUT" = "true" ]; then
        "$PYTHON" -c "
import json
report = {
    'strategy': '${target_strategy}',
    'runs': ${test_runs},
    'successful_runs': ${successful_runs},
    'gateway_proxy_events': ${proxy_events_count},
    'generation_attempts': ${gen_attempts_count},
    'successful_generation_dispatches': ${gen_dispatches_count},
    'auxiliary_gateway_events': ${aux_events_count},
    'failovers_count': ${failovers_count},
    'dispatches_recorded': ${gen_dispatches_count},
    'generation_sequence': json.load(open('${TMP_DIR}/dispatches.json')),
    'verification': json.loads('''${verify_res}'''),
    'hits_delta': json.load(open('${TMP_DIR}/hits_delta.json')),
    'concurrent_traffic': json.loads('''${conc_res}'''),
    'passed': ('${verify_passed}' == 'true')
}
print(json.dumps(report, indent=2))
"
    fi

    if [ "$verify_passed" != "true" ]; then
        return 1
    fi
    return 0
}

# Subcommand: gateway
cmd_gateway() {
    local runs="${RUNS:-1}"
    [ "$runs" -le 0 ] && runs=1
    run_strategy_test "max_quota" "$runs"
}

# Subcommand: round-robin
cmd_round_robin() {
    run_strategy_test "round_robin" "$RUNS"
}

# Subcommand: least-used
cmd_least_used() {
    run_strategy_test "least_used" "$RUNS"
}

# Subcommand: max-quota
cmd_max_quota() {
    run_strategy_test "max_quota" "$RUNS"
}

# Subcommand: scheduler
cmd_scheduler() {
    local strat
    strat="$(get_current_strategy)"
    echo -e "Testing active pool strategy: ${CLR_BOLD}${strat}${CLR_RESET}"
    run_strategy_test "$strat" "$RUNS"
}

# Subcommand: all
cmd_all() {
    echo -e "${CLR_BOLD}${CLR_CYAN}====================================================================${CLR_RESET}"
    echo -e "${CLR_BOLD}${CLR_CYAN}              agy-pool Comprehensive Live Test Suite                ${CLR_RESET}"
    echo -e "${CLR_BOLD}${CLR_CYAN}====================================================================${CLR_RESET}"
    cmd_doctor
    echo ""
    cmd_port
    echo ""
    run_strategy_test "round_robin" "${RUNS:-2}"
}

# Parse CLI arguments
parse_args() {
    while [ $# -gt 0 ]; do
        case "$1" in
            doctor|check|health)
                COMMAND="doctor"
                shift
                ;;
            port)
                COMMAND="port"
                shift
                ;;
            status)
                COMMAND="status"
                shift
                ;;
            gateway)
                COMMAND="gateway"
                shift
                ;;
            round-robin|rr)
                COMMAND="round_robin"
                shift
                ;;
            least-used|lu)
                COMMAND="least_used"
                shift
                ;;
            max-quota|mq)
                COMMAND="max_quota"
                shift
                ;;
            scheduler|sched)
                COMMAND="scheduler"
                shift
                ;;
            all)
                COMMAND="all"
                shift
                ;;
            -n|--runs)
                RUNS="$2"
                shift 2
                ;;
            -t|--timeout)
                TIMEOUT="$2"
                shift 2
                ;;
            -k|--keep-artifacts)
                KEEP_ARTIFACTS=true
                shift
                ;;
            -j|--json)
                JSON_OUTPUT=true
                shift
                ;;
            -y|--yes)
                AUTO_CONFIRM=true
                shift
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            *)
                echo -e "${CLR_RED}[Error] Unknown argument: $1${CLR_RESET}" >&2
                usage >&2
                exit 1
                ;;
        esac
    done
}

main() {
    parse_args "$@"
    check_environment

    if [ -z "$COMMAND" ]; then
        echo -e "${CLR_BOLD}${CLR_CYAN}====================================================================${CLR_RESET}"
        echo -e "${CLR_BOLD}${CLR_CYAN}             agy-pool Live Integration Smoke Test Harness           ${CLR_RESET}"
        echo -e "${CLR_BOLD}${CLR_CYAN}====================================================================${CLR_RESET}"
        echo -e "\nRunning safe environment diagnostics (consumes 0 quota)...\n"
        cmd_doctor
        echo ""
        cmd_port
        echo ""
        echo -e "${CLR_YELLOW}--------------------------------------------------------------------${CLR_RESET}"
        echo -e "${CLR_BOLD}To execute live quota-consuming generation tests, run:${CLR_RESET}"
        echo -e "  $(basename "$0") gateway       # Single request smoke test"
        echo -e "  $(basename "$0") round-robin   # Multi-request rotation test"
        echo -e "  $(basename "$0") least-used    # Lowest-hit distribution test"
        echo -e "  $(basename "$0") max-quota     # Highest-quota priority test"
        echo -e "  $(basename "$0") all           # Full validation suite"
        echo -e "${CLR_YELLOW}--------------------------------------------------------------------${CLR_RESET}"
        exit 0
    fi

    case "$COMMAND" in
        doctor)
            cmd_doctor
            ;;
        port)
            cmd_port
            ;;
        status)
            cmd_status
            ;;
        gateway)
            cmd_gateway
            ;;
        round_robin)
            cmd_round_robin
            ;;
        least_used)
            cmd_least_used
            ;;
        max_quota)
            cmd_max_quota
            ;;
        scheduler)
            cmd_scheduler
            ;;
        all)
            cmd_all
            ;;
    esac
}

main "$@"
