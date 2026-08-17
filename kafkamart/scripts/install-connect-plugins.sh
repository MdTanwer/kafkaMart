#!/usr/bin/env bash
# Install Kafka Connect plugins into infra/connect-plugins (mounted on the worker).
# Safe to re-run. Requires Docker (uses cp-kafka-connect which ships confluent-hub).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PLUGIN_DIR="${ROOT}/infra/connect-plugins"
CONNECT_IMAGE="${CONNECT_IMAGE:-confluentinc/cp-kafka-connect:7.8.0}"
JDBC_VERSION="${JDBC_CONNECTOR_VERSION:-10.8.1}"
ES_VERSION="${ES_CONNECTOR_VERSION:-14.1.2}"
DEBEZIUM_VERSION="${DEBEZIUM_CONNECTOR_VERSION:-2.7.3}"

mkdir -p "${PLUGIN_DIR}"

hub() {
  local coord="$1"
  echo "confluent-hub install ${coord}"
  docker run --rm \
    -v "${PLUGIN_DIR}:/usr/share/kafkamart-plugins" \
    "${CONNECT_IMAGE}" \
    confluent-hub install --no-prompt --component-dir /usr/share/kafkamart-plugins "${coord}"
}

hub "confluentinc/kafka-connect-jdbc:${JDBC_VERSION}"
hub "confluentinc/kafka-connect-elasticsearch:${ES_VERSION}"
hub "debezium/debezium-connector-postgresql:${DEBEZIUM_VERSION}"

echo "Building kafkamart InferConnectSchema SMT"
export JAVA_HOME="${JAVA_HOME:-/data/tools/jdk-25}"
"${ROOT}/mvnw" -q -f "${ROOT}/infra/connect-transforms/pom.xml" package
SMT_DIR="${PLUGIN_DIR}/kafkamart-json-smt"
mkdir -p "${SMT_DIR}/lib"
cp "${ROOT}/infra/connect-transforms/target/kafkamart-connect-json-smt-1.0.0-SNAPSHOT.jar" \
  "${SMT_DIR}/lib/"

echo
echo "Plugins in ${PLUGIN_DIR}:"
ls -1 "${PLUGIN_DIR}"
echo "Restart Connect so it picks up new plugins: docker compose restart connect"
