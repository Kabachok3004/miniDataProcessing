"""
Сабмит PyFlink-джоба на удалённый Flink на VPS.

Стратегия: джобы лежат в репо в datalake/streaming/jobs/ и примонтированы
в JM-контейнер как /opt/jobs (см. docker-compose.yml). Чтобы запустить, делаем
ssh + docker compose exec.

Использование:
    export VPS_SSH=user@host
    python -m datalake.streaming.flink_submit rolling_metrics.py
    python -m datalake.streaming.flink_submit rolling_metrics.py --detach

Так же поддерживает list/cancel:
    python -m datalake.streaming.flink_submit --list
    python -m datalake.streaming.flink_submit --cancel <job_id>
"""
import argparse
import os
import shlex
import subprocess
import sys


COMPOSE_DIR = "/opt/mini_data_proc/datalake/deploy"
JOBS_DIR_IN_CONTAINER = "/opt/jobs"


def _ssh_target() -> str:
    target = os.getenv("VPS_SSH")
    if not target:
        sys.exit("VPS_SSH не задан. Пример: export VPS_SSH=ubuntu@1.2.3.4")
    return target


def _remote(cmd: str) -> int:
    full = f"cd {COMPOSE_DIR} && {cmd}"
    print(f"[ssh {_ssh_target()}] {full}", flush=True)
    return subprocess.call(["ssh", _ssh_target(), full])


def submit(job_filename: str, detach: bool) -> int:
    detach_flag = "-d" if detach else ""
    job_path = f"{JOBS_DIR_IN_CONTAINER}/{job_filename}"
    cmd = (
        "docker compose exec -T flink-jobmanager "
        f"flink run {detach_flag} -py {shlex.quote(job_path)}"
    )
    return _remote(cmd)


def list_jobs() -> int:
    return _remote("docker compose exec -T flink-jobmanager flink list")


def cancel(job_id: str) -> int:
    return _remote(
        f"docker compose exec -T flink-jobmanager flink cancel {shlex.quote(job_id)}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("job", nargs="?", help="имя файла из datalake/streaming/jobs/")
    parser.add_argument("--detach", action="store_true", help="вернуть control сразу")
    parser.add_argument("--list", action="store_true", help="показать активные джобы")
    parser.add_argument("--cancel", metavar="JOB_ID", help="отменить джоб")
    args = parser.parse_args()

    if args.list:
        sys.exit(list_jobs())
    if args.cancel:
        sys.exit(cancel(args.cancel))
    if not args.job:
        parser.error("укажи имя файла джоба, --list или --cancel")
    sys.exit(submit(args.job, args.detach))


if __name__ == "__main__":
    main()
