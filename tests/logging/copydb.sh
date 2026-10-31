#! /bin/bash

set -e
set -u
set -o pipefail

results=/tmp/results
expected=./expected
mkdir -p "${results}"

normalize_text() {
    tr -d '\r' \
        | sed -E \
              -e 's/^(\x1b\[[0-9;]*m)?[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9:.]+ [0-9]+ /TS PID /' \
              -e 's/^(\x1b\[[0-9;]*m)?[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]+ [0-9]+ /TS PID /' \
              -e 's/(\x1b\[90m)?[A-Za-z_]+\.c:[0-9]+/\1FILE:LINE/' \
              -e 's/Running pgcopydb version .* from .*$/Running pgcopydb version VERSION/' \
              -e 's/ipcrm -s [0-9]+/ipcrm -s N/' \
              -e 's/\[(SOURCE|TARGET) [0-9]+\]/[\1 BPID]/g'
}

golden_diff() {
    diff -u <(sed -E 's/FILE:LINE +/FILE:LINE /' "$1") \
            <(sed -E 's/FILE:LINE +/FILE:LINE /' "$2")
}

normalize_json() {
    sed -E \
        -e 's/"timestamp":"[^"]*"/"timestamp":"TS"/' \
        -e 's/"pid":[0-9]+/"pid":0/' \
        -e 's/"file_line_num":[0-9]+/"file_line_num":0/' \
        -e 's/"message":"Running pgcopydb version (\\.|[^"\\])*"/"message":"Running pgcopydb version VERSION"/' \
        -e 's/ipcrm -s [0-9]+/ipcrm -s N/'
}

sample_outputs() {
    pgcopydb internal log-sample 2> "${results}/raw-stderr-text.out"
    normalize_text < "${results}/raw-stderr-text.out" > "${results}/stderr-text.out"

    script -qec "pgcopydb internal log-sample" /dev/null > "${results}/raw-stderr-tty.out"
    normalize_text < "${results}/raw-stderr-tty.out" > "${results}/stderr-tty.out"

    PGCOPYDB_LOG_JSON=true pgcopydb internal log-sample 2> "${results}/raw-stderr-json.out"
    normalize_json < "${results}/raw-stderr-json.out" > "${results}/stderr-json.out"

    PGCOPYDB_LOG_FILENAME="${results}/raw-file-text.out" \
        pgcopydb internal log-sample 2> /dev/null
    normalize_text < "${results}/raw-file-text.out" > "${results}/file-text.out"

    PGCOPYDB_LOG_FILENAME="${results}/raw-file-json.out" PGCOPYDB_LOG_JSON_FILE=true \
        pgcopydb internal log-sample 2> /dev/null
    normalize_json < "${results}/raw-file-json.out" > "${results}/file-json.out"
}

check_stress() {
    local label="$1" file="$2" procs="$3" count="$4" size="$5"

    awk -v S="${size}" -v N=$((procs * count)) -v label="${label}" '
        /Running pgcopydb version/ { next }
        {
            line = $0
            records = gsub(/stress pid=/, "&", line)
            if (records != 1 || !match($0, /stress pid=[0-9]+ seq=[0-9]+ size=[0-9]+ payload=[a-z]* END/)) {
                bad++
                next
            }
            split(substr($0, RSTART, RLENGTH), f, /[ =]/)
            payload = f[9]
            if (f[7] != S || length(payload) != S) { bad++; next }
            c = sprintf("%c", 97 + f[5] % 26)
            gsub(c, "", payload)
            if (payload != "") { bad++; next }
            key = f[3] SUBSEP f[5]
            if (key in seen) { dup++; next }
            seen[key] = 1
            ok++
        }
        END {
            printf "%s: %d valid, %d torn, %d duplicate, %d expected\n", label, ok, bad, dup, N
            complete = (N == 0) ? (ok > 0) : (ok == N)
            exit !(complete && bad == 0 && dup == 0)
        }' "${file}"
}

quit() {
    local failed=0
    local file="${results}/quit-file.log"
    local semaphores_before semaphores_after

    semaphores_before=$(ipcs -s | grep -c '^0x' || true)

    PGCOPYDB_LOG_FILENAME="${file}" \
        pgcopydb internal log-stress 8 1000000 200 2> /dev/null &
    local pid=$!

    sleep 2
    kill -QUIT -- "-${pid}"

    local rc=0
    wait "${pid}" || rc=$?
    expect_equal "quit exit code" "0" "${rc}" || failed=1

    check_stress "quit log file" "${file}" 0 0 200 || failed=1

    semaphores_after=$(ipcs -s | grep -c '^0x' || true)
    expect_equal "quit semaphores released" "${semaphores_before}" "${semaphores_after}" \
        || failed=1

    return "${failed}"
}

run_stress() {
    local label="$1"
    shift

    if ! "$@"; then
        echo "${label}: pgcopydb internal log-stress failed"
        return 1
    fi
}

stress() {
    local procs="$1" count="$2" size="$3" failed=0
    local tag="${procs}x${count}x${size}"
    local cmd=(pgcopydb internal log-stress "${procs}" "${count}" "${size}")

    run_stress "stderr file ${tag}" "${cmd[@]}" \
               2> "${results}/stress-stderr-file-${tag}.log" || failed=1
    check_stress "stderr file ${tag}" "${results}/stress-stderr-file-${tag}.log" \
                 "${procs}" "${count}" "${size}" || failed=1

    run_stress "stderr pipe ${tag}" "${cmd[@]}" 2>&1 \
        | cat > "${results}/stress-stderr-pipe-${tag}.log" || failed=1
    check_stress "stderr pipe ${tag}" "${results}/stress-stderr-pipe-${tag}.log" \
                 "${procs}" "${count}" "${size}" || failed=1

    PGCOPYDB_LOG_FILENAME="${results}/stress-file-text-${tag}.log" \
        run_stress "log file text ${tag}" "${cmd[@]}" 2> /dev/null || failed=1
    check_stress "log file text ${tag}" "${results}/stress-file-text-${tag}.log" \
                 "${procs}" "${count}" "${size}" || failed=1

    PGCOPYDB_LOG_FILENAME="${results}/stress-file-json-${tag}.log" PGCOPYDB_LOG_JSON_FILE=true \
        run_stress "log file json ${tag}" "${cmd[@]}" 2> /dev/null || failed=1
    check_stress "log file json ${tag}" "${results}/stress-file-json-${tag}.log" \
                 "${procs}" "${count}" "${size}" || failed=1

    rm -f "${results}"/stress-*-"${tag}".log
    return "${failed}"
}

expect_equal() {
    local label="$1" expected_value="$2" actual_value="$3"

    if [ "${expected_value}" != "${actual_value}" ]; then
        echo "${label}: expected [${expected_value}], got [${actual_value}]"
        return 1
    fi

    echo "${label}: ok"
}

extra() {
    local failed=0
    local json="${results}/extra-json.log"
    local file="${results}/extra-file.log"

    PGCOPYDB_LOG_JSON=true pgcopydb internal log-extra 2> "${json}" || failed=1

    expect_equal "extra json lines parse" "0" \
                 "$(jq -c . "${json}" > /dev/null 2>&1; echo $?)" || failed=1

    expect_equal "extra large message length" "20018" \
                 "$(jq -r 'select(.message | startswith("large sample")) | .message | length' "${json}")" \
        || failed=1

    expect_equal "extra invalid utf8" \
                 "$(printf 'invalid utf8 sample: \xef\xbf\xbd\xef\xbf\xbd end')" \
                 "$(jq -r 'select(.message | startswith("invalid utf8")) | .message' "${json}")" \
        || failed=1

    expect_equal "extra errno kept" "errno kept sample: Permission denied" \
                 "$(jq -r 'select(.message | startswith("errno kept sample: P")) | .message' "${json}")" \
        || failed=1

    PGCOPYDB_LOG_FILENAME="${file}" pgcopydb internal log-extra 2>&1 | head -n 1 > /dev/null
    expect_equal "extra closed stderr exit" "0" "${PIPESTATUS[0]}" || failed=1

    expect_equal "extra closed stderr file complete" "1" \
                 "$(grep -c 'log-extra done' "${file}")" || failed=1

    expect_equal "extra closed stderr reported" "1" \
                 "$(grep -c 'Failed to write to the log on fd 2: Broken pipe' "${file}")" \
        || failed=1

    return "${failed}"
}

workdir() {
    local failed=0
    local dir=/tmp/workdir
    local jsonl="${dir}/log/pgcopydb.jsonl"

    pgcopydb ping

    psql -q -d "${PGCOPYDB_SOURCE_PGURI}" <<'EOF'
create table logging_sample(id int primary key, v text);
insert into logging_sample select g, md5(g::text) from generate_series(1, 1000) g;
EOF

    pgcopydb clone --dir "${dir}" --table-jobs 2 --index-jobs 2 || failed=1

    expect_equal "workdir jsonl parses" "0" \
                 "$(jq -c . "${jsonl}" > /dev/null 2>&1; echo $?)" || failed=1

    expect_equal "workdir first record replayed" "true" \
                 "$(head -n 1 "${jsonl}" | jq '.message | startswith("Running pgcopydb version")')" \
        || failed=1

    expect_equal "workdir records without run_id" "0" \
                 "$(jq -c 'select(.run_id == null)' "${jsonl}" | wc -l)" || failed=1

    expect_equal "workdir records below info" "0" \
                 "$(jq -c 'select(.error_level < 5)' "${jsonl}" | wc -l)" || failed=1

    expect_equal "workdir records with a role" "true" \
                 "$(jq -s 'map(select(.role != null)) | length > 0' "${jsonl}")" || failed=1

    pgcopydb compare schema --dir "${dir}" || failed=1

    expect_equal "workdir appends with a new run_id" "2" \
                 "$(jq -r .run_id "${jsonl}" | sort -u | wc -l)" || failed=1

    PGCOPYDB_LOG_WORKDIR=off pgcopydb compare schema --dir /tmp/workdir-off || failed=1
    expect_equal "workdir off" "absent" \
                 "$(test -e /tmp/workdir-off/log/pgcopydb.jsonl && echo present || echo absent)" \
        || failed=1

    pgcopydb compare schema --dir /tmp/workdir-verbose -v || failed=1
    expect_equal "workdir follows -v" "true" \
                 "$(jq -s 'map(select(.error_level == 4)) | length > 0' /tmp/workdir-verbose/log/pgcopydb.jsonl)" \
        || failed=1

    return "${failed}"
}

pg_errors() {
    local failed=0
    local json="${results}/pg-errors.json"

    pgcopydb internal pg-errors 2> "${results}/raw-pg-errors-text.out" || failed=1
    normalize_text < "${results}/raw-pg-errors-text.out" > "${results}/pg-errors-text.out"

    if ! golden_diff "${expected}/pg-errors-text.out" "${results}/pg-errors-text.out"; then
        echo "golden mismatch: pg-errors-text"
        failed=1
    fi

    PGCOPYDB_LOG_JSON=true pgcopydb internal pg-errors 2> "${json}" || failed=1

    expect_equal "pg errors json parses" "0" \
                 "$(jq -c . "${json}" > /dev/null 2>&1; echo $?)" || failed=1

    expect_equal "pg errors raise fields" "P0001|sample detail|sample hint|source" \
                 "$(jq -r 'select(.sqlstate == "P0001" and .pg_message != null) | [.sqlstate, .pg_detail, .pg_hint, .endpoint] | join("|")' "${json}")" \
        || failed=1

    expect_equal "pg errors division logged once" "1" \
                 "$(jq -c 'select(.pg_message == "division by zero")' "${json}" | wc -l)" || failed=1

    expect_equal "pg errors query has sqlstate" "22012|'42'" \
                 "$(jq -r 'select(.query == "select $1::int / 0") | [.sqlstate, .params] | join("|")' "${json}")" \
        || failed=1

    expect_equal "pg errors copy sqlstate" "42P01" \
                 "$(jq -r 'select(.message | contains("logging_does_not_exist")) | .sqlstate' "${json}")" \
        || failed=1

    expect_equal "pg errors connection endpoint" "target" \
                 "$(jq -r 'select(.message | startswith("Connection to database failed")) | .endpoint' "${json}")" \
        || failed=1

    return "${failed}"
}

subprocess_lines() {
    local failed=0
    local text="${results}/lines-text.log"
    local json="${results}/lines-json.log"
    local script='
        for i in $(seq 1 20); do
            printf "pg_restore: warning: split line %d " "$i" >&2
            head -c 3000 /dev/zero | tr "\0" x >&2
            sleep 0.01
            printf " END\n" >&2
            printf "stdout line %d\n" "$i"
        done
        printf "no newline at end" >&2'

    pgcopydb internal run-program /bin/bash -c "${script}" 2> "${text}" || failed=1

    expect_equal "lines whole warnings" "20" \
                 "$(grep -cE ' WARN +[^ ]+ +pg_restore: warning: split line [0-9]+ x{3000} END$' "${text}")" \
        || failed=1

    expect_equal "lines stdout" "20" \
                 "$(grep -cE ' INFO +[^ ]+ +stdout line [0-9]+$' "${text}")" || failed=1

    expect_equal "lines last line without newline" "1" \
                 "$(grep -cE ' ERROR +[^ ]+ +no newline at end$' "${text}")" || failed=1

    expect_equal "lines total" "42" \
                 "$(grep -vc 'Running pgcopydb version' "${text}")" || failed=1

    PGCOPYDB_LOG_JSON=true pgcopydb internal run-program /bin/bash -c "${script}" \
        2> "${json}" || failed=1

    expect_equal "lines origin" "41" \
                 "$(jq -c 'select(.origin == "bash")' "${json}" | wc -l)" || failed=1

    return "${failed}"
}

restore_tolerance() {
    local failed=0
    local src="${PGCOPYDB_SOURCE_PGURI%/*}/restore_src"
    local dst="${PGCOPYDB_TARGET_PGURI%/*}/restore_dst"
    local log="${results}/restore-tolerance.log"

    psql -q -d "${PGCOPYDB_SOURCE_PGURI}" -c 'create role logging_reader'
    psql -q -d "${PGCOPYDB_SOURCE_PGURI}" -c 'create database restore_src'
    psql -q -d "${PGCOPYDB_TARGET_PGURI}" -c 'create database restore_dst'

    psql -q -d "${src}" <<'EOF'
do $$
begin
  for i in 1..8 loop
    execute format('create table t%s(id int primary key)', i);
    execute format('create policy p%s on t%s to logging_reader using (true)', i, i);
  end loop;
end
$$;
EOF

    pgcopydb clone --source "${src}" --target "${dst}" \
             --dir /tmp/restore-tolerance --restore-jobs 4 2> "${log}" || failed=1

    grep -E 'ignored errors|errors ignored on restore' "${log}" || true

    expect_equal "restore tolerance failed statements" "1" \
                 "$(grep -cE 'reported [0-9]+ ignored errors, 8 failed statements \(within tolerance of 10\)' "${log}")" \
        || failed=1

    return "${failed}"
}

log_files() {
    local failed=0
    local errors="${results}/errors.log"
    local levelled="${results}/levelled.log"

    PGCOPYDB_LOG_ERROR_FILENAME="${errors}" pgcopydb internal log-sample 2> /dev/null || failed=1

    expect_equal "error file levels" "3" \
                 "$(grep -cE '^[0-9-]+ [0-9:.]+ [0-9]+ (ERROR|FATAL) ' "${errors}")" || failed=1

    expect_equal "error file only errors" "3" "$(wc -l < "${errors}")" || failed=1

    PGCOPYDB_LOG_ERROR_FILENAME="${errors}" pgcopydb internal log-sample 2> /dev/null || failed=1
    expect_equal "error file appends" "6" "$(wc -l < "${errors}")" || failed=1

    PGCOPYDB_LOG_FILENAME="${levelled}" PGCOPYDB_LOG_FILE_LEVEL=warn \
        pgcopydb internal log-sample 2> /dev/null || failed=1

    expect_equal "file level warn and above" "5" \
                 "$(grep -cE '^[0-9-]+ [0-9:.]+ [0-9]+ (WARN|ERROR|FATAL) ' "${levelled}")" || failed=1

    expect_equal "file level below warn" "0" \
                 "$(grep -cE '^[0-9-]+ [0-9:.]+ [0-9]+ (TRACE|SQLite|DEBUG|SQL|NOTICE|INFO) ' "${levelled}")" \
        || failed=1

    local rc=0
    PGCOPYDB_LOG_FILENAME="${levelled}" PGCOPYDB_LOG_FILE_LEVEL=loud \
        pgcopydb internal log-sample > /dev/null 2>&1 || rc=$?
    expect_equal "file level invalid" "1" "${rc}" || failed=1

    return "${failed}"
}

sample_outputs

if [ -n "${LOGTEST_GENERATE:-}" ]; then
    for f in stderr-text stderr-tty stderr-json file-text file-json; do
        cp "${results}/${f}.out" "${LOGTEST_GENERATE}/${f}.out"
    done
    exit 0
fi

failed=0

for f in stderr-text stderr-tty stderr-json file-text file-json; do
    if ! golden_diff "${expected}/${f}.out" "${results}/${f}.out"; then
        echo "golden mismatch: ${f}"
        failed=1
    fi
done

extra || failed=1
log_files || failed=1
quit || failed=1
workdir || failed=1
pg_errors || failed=1
subprocess_lines || failed=1
restore_tolerance || failed=1

stress 16 2000 100 || failed=1
stress 16 2000 3000 || failed=1
stress 16 500 5000 || failed=1
stress 4 50 70000 || failed=1

exit "${failed}"
