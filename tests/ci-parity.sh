#! /usr/bin/env bash
#
# Run the same suites and Postgres versions as .github/workflows/run-tests-tiered.yml.
#
# Images are pulled first: CI pulls fresh every run, so a stale local image
# tests a different Postgres minor version than CI does.
#
# Usage:
#   ./ci-parity.sh                  all versions, all suites
#   ./ci-parity.sh 18               one version
#   ./ci-parity.sh 18 cdc-pgoutput  one version, some suites

set -u

cd "$(dirname "$0")"

WORKFLOW=../.github/workflows/run-tests-tiered.yml
LOGDIR=${LOGDIR:-/tmp/ci-parity}

if [ ! -f "${WORKFLOW}" ]
then
    echo "cannot find ${WORKFLOW}" >&2
    exit 1
fi

# the workflow is the single source of truth for the matrix
ci_versions()
{
    awk '/^  integration_tests:/,/^    steps:/' "${WORKFLOW}" \
        | grep -E '^          - [0-9]+$' | tr -d ' -'
}

ci_suites()
{
    awk '/^  integration_tests:/,/^    steps:/' "${WORKFLOW}" \
        | grep -E '^          - ' | sed 's/^          - //' \
        | grep -vE '^[0-9]+$'
}

VERSIONS=$(ci_versions)
SUITES="unit $(ci_suites | tr '\n' ' ')"

if [ $# -ge 1 ]
then
    VERSIONS=$1
    shift
fi

if [ $# -ge 1 ]
then
    SUITES=$*
fi

mkdir -p "${LOGDIR}"

echo "versions: ${VERSIONS}"
echo "suites:   $(echo ${SUITES} | wc -w | tr -d ' ')"
echo "logs:     ${LOGDIR}"
echo

# CI always pulls, so we do too
for v in ${VERSIONS}
do
    echo "pulling postgres:${v}"
    docker pull -q "postgres:${v}" >/dev/null || echo "  pull failed, using cached"
done
echo

rc=0
summary=${LOGDIR}/summary.txt
: > "${summary}"

for v in ${VERSIONS}
do
    for s in ${SUITES}
    do
        log=${LOGDIR}/pg${v}-${s}.log
        printf "pg%-3s %-34s " "${v}" "${s}"

        start=$SECONDS
        PGVERSION=${v} make "${s}" > "${log}" 2>&1
        status=$?
        took=$((SECONDS - start))

        # The suite Makefiles use "test: down run down", and make dedupes
        # prerequisites, so the trailing down never runs and the containers
        # keep their networks. Docker then runs out of address pools.
        # -v also drops the anonymous PGDATA volumes, which otherwise pile up
        # into hundreds of gigabytes across a full matrix run
        if [ -f "${s}/compose.yaml" ]
        then
            (cd "${s}" && PGVERSION=${v} docker compose down --remove-orphans -v) \
                >/dev/null 2>&1
        fi
        docker network prune -f >/dev/null 2>&1
        docker volume prune -f >/dev/null 2>&1

        if [ ${status} -eq 0 ]
        then
            printf "ok    %3ds\n" "${took}"
            echo "ok   pg${v} ${s}" >> "${summary}"
        else
            printf "FAIL  %3ds  %s\n" "${took}" "${log}"
            echo "FAIL pg${v} ${s}" >> "${summary}"
            rc=1
        fi
    done
done

docker builder prune -f >/dev/null 2>&1

echo
echo "===================== summary ====================="
grep -c '^ok'   "${summary}" | sed 's/^/passed: /'
grep -c '^FAIL' "${summary}" | sed 's/^/failed: /'
grep '^FAIL' "${summary}" || true

exit ${rc}
