#! /bin/bash

set -e
set -u
set -o pipefail

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"

if [ -z "${PGCOPYDB:-}" ]; then
    PGCOPYDB="${SCRIPT_DIR}/../src/bin/pgcopydb/pgcopydb"
fi

echo "Updating help messages using binary ${PGCOPYDB}"

function help_text() {
    local cmd="$1"
    local out
    local rc

    case "${cmd}" in
        "pgcopydb")
            out="$(${PGCOPYDB} --help 2>&1)"
            rc=$?
            ;;
        "pgcopydb help")
            out="$(${PGCOPYDB} help 2>&1)"
            rc=$?
            ;;
        *)
            # shellcheck disable=SC2086
            out="$(${PGCOPYDB} ${cmd#pgcopydb } --help 2>&1)"
            rc=$?
            ;;
    esac

    if [ ${rc} -ne 0 ]; then
        echo "error: \"${cmd}\" exited with status ${rc}" >&2
        printf '%s\n' "${out}" >&2
        return 1
    fi

    printf '%s\n' "${out}" | grep -v 'Running pgcopydb version' || true
}

function update_file() {
    local file="$1"
    local tmp
    tmp="$(mktemp)"

    local in_block=0
    local count=0
    local cmd=""
    local text=""

    while IFS= read -r line || [ -n "${line}" ]; do
        if [[ ${line} =~ ^\<!--\ BEGIN\ HELP:\ (.+)\ --\>$ ]]; then
            cmd="${BASH_REMATCH[1]}"
            count=$((count + 1))

            if ! text="$(help_text "${cmd}")"; then
                rm -f "${tmp}"
                echo "error: ${file} asks for help of \"${cmd}\"" >&2
                return 1
            fi

            {
                printf '%s\n' "${line}"
                printf '```\n'
                printf '%s\n' "${text}"
                printf '```\n'
            } >>"${tmp}"

            in_block=1
            continue
        fi

        if [[ ${line} == "<!-- END HELP -->" ]]; then
            in_block=0
            printf '%s\n' "${line}" >>"${tmp}"
            continue
        fi

        if [ ${in_block} -eq 0 ]; then
            printf '%s\n' "${line}" >>"${tmp}"
        fi
    done <"${file}"

    if [ ${in_block} -ne 0 ]; then
        rm -f "${tmp}"
        echo "error: ${file} has a BEGIN HELP marker with no END HELP" >&2
        return 1
    fi

    mv "${tmp}" "${file}"
    echo "  ${file}: ${count} help block(s)"
}

found=0

while IFS= read -r file; do
    found=$((found + 1))
    update_file "${file}" || exit 1
done < <(grep -rl --include='*.md' '<!-- BEGIN HELP: ' "${SCRIPT_DIR}" | sort)

if [ ${found} -eq 0 ]; then
    echo "error: no documentation page asks for a help block" >&2
    exit 1
fi
