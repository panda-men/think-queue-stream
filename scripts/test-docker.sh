#!/bin/sh
set -eu
task_php_version="${1:-8.2}"
task_redis_version="${2:-6.2}"
case "$task_php_version" in 8.2|8.3|8.4) ;; *) echo 'PHP version must be 8.2, 8.3 or 8.4' >&2; exit 1 ;; esac
case "$task_redis_version" in 6.2|7.4) ;; *) echo 'Redis version must be 6.2 or 7.4' >&2; exit 1 ;; esac
task_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
task_network="think-queue-stream-test-$$"
task_redis="think-queue-stream-redis-$$"
task_php="think-queue-stream-php-$$"
cleanup() {
    docker rm -f "$task_php" >/dev/null 2>&1 || true
    docker rm -f "$task_redis" >/dev/null 2>&1 || true
    docker network rm "$task_network" >/dev/null 2>&1 || true
}
trap cleanup EXIT HUP INT TERM
docker build -f "$task_root/tests/Dockerfile" --build-arg "PHP_VERSION=$task_php_version" \
    -t "think-queue-stream-test:$task_php_version" "$task_root"
docker network create "$task_network" >/dev/null
docker run -d --name "$task_redis" --network "$task_network" "redis:$task_redis_version-alpine" >/dev/null
docker run -d --name "$task_php" --network "$task_network" -e "REDIS_HOST=$task_redis" \
    "think-queue-stream-test:$task_php_version" sleep infinity >/dev/null
# 使用复制而非宿主机 bind mount，也兼容远程 Docker 和限制了挂载目录的 Colima。
COPYFILE_DISABLE=1 tar --no-xattrs -C "$task_root" --exclude=.git --exclude=.idea --exclude=vendor --exclude=.phpunit.cache -cf - . | docker cp - "$task_php:/package"
docker exec "$task_php" sh -c 'composer install --no-interaction && composer validate --strict && composer lint && composer test'
