#! /bin/bash

set -e
set -u
set -o pipefail

if [ $# -ne 1 ]; then
    echo "usage: $0 <tag>" >&2
    echo "example: $0 v0.20.0" >&2
    exit 64
fi

TAG="$1"
SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
CHANGELOG="${SCRIPT_DIR}/../CHANGELOG.md"
REPO_URL="https://github.com/planetscale/pgcopydb"

if [ ! -f "${CHANGELOG}" ]; then
    echo "error: ${CHANGELOG} not found" >&2
    exit 1
fi

VERSION="${TAG#v}"
SHORT="$(echo "${VERSION}" | cut -d. -f1,2)"

section() {
    local want="$1"

    awk -v want="${want}" '
        /^### pgcopydb v/ {
            if (found) { exit }
            split($0, f, " ")
            if (f[3] == want) { found = 1; next }
        }
        found { print }
    ' "${CHANGELOG}"
}

BODY="$(section "v${VERSION}")"

if [ -z "${BODY}" ]; then
    BODY="$(section "v${SHORT}")"
fi

if [ -z "${BODY}" ]; then
    echo "error: CHANGELOG.md has no section for v${VERSION} or v${SHORT}" >&2
    echo "add one before tagging, the release notes come from it" >&2
    exit 1
fi

PREVIOUS=""

if git rev-parse -q --verify "refs/tags/${TAG}" >/dev/null 2>&1; then
    PREVIOUS="$(git tag --sort=-v:refname --list 'v[0-9]*' |
                    grep -A1 -x -- "${TAG}" | tail -n +2 | head -1 || true)"
fi

{
    printf '%s\n' "${BODY}" |
        awk '
            /^### / { sub(/^### /, "## "); sub(/[ \t]*#+[ \t]*$/, "") }
            /^\* /  { sub(/^\* /, "- ") }
            { print }
        ' |
        awk 'BEGIN { blank = 1 }
             NF == 0 { if (!blank) { print }; blank = 1; next }
             { print; blank = 0 }'

    if [ -n "${PREVIOUS}" ]; then
        printf '\n**Full changelog:** %s/compare/%s...%s\n' \
               "${REPO_URL}" "${PREVIOUS}" "${TAG}"
    fi
}
