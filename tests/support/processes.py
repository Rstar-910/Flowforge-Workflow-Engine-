"""Spawn real FlowForge processes for failure-injection tests."""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def process_env(db_url: str, signal_dir: Path, **extra: str) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith("FLOWFORGE_")}
    env.update(
        {
            "FLOWFORGE_DATABASE_URL": db_url,
            "FLOWFORGE_HANDLER_MODULES": "flowforge.handlers.builtin,tests.support.handlers",
            "FLOWFORGE_LEASE_SECONDS": "2",
            "FLOWFORGE_HEARTBEAT_INTERVAL_SECONDS": "0.4",
            "FLOWFORGE_WORKER_POLL_INTERVAL_SECONDS": "0.05",
            "FLOWFORGE_WORKER_MAX_POLL_INTERVAL_SECONDS": "0.2",
            "FLOWFORGE_SCHEDULER_INTERVAL_SECONDS": "0.2",
            "FLOWFORGE_WORKER_SHUTDOWN_GRACE_SECONDS": "10",
            "FLOWFORGE_DB_CONNECT_TIMEOUT_SECONDS": "1",
            "FLOWFORGE_LOG_FORMAT": "text",
            "FLOWFORGE_API_KEYS": "admin-key:tenant-a:admin,user-key:tenant-a:user",
            "FLOWFORGE_TEST_SIGNAL_DIR": str(signal_dir),
            "PYTHONPATH": str(ROOT),
            "PYTHONUNBUFFERED": "1",
        }
    )
    env.update(extra)
    return env


class Proc:
    def __init__(self, args: list[str], env: dict[str, str], log_path: Path) -> None:
        self.log_path = log_path
        self._log = log_path.open("w")
        self.popen = subprocess.Popen(
            [sys.executable, "-m", "flowforge", *args],
            env=env,
            cwd=ROOT,
            stdout=self._log,
            stderr=subprocess.STDOUT,
        )

    @property
    def pid(self) -> int:
        return self.popen.pid

    def signal(self, sig: int) -> None:
        if self.popen.poll() is None:
            os.kill(self.popen.pid, sig)

    def kill(self) -> None:
        self.signal(signal.SIGKILL)
        self.popen.wait(10)

    async def wait_exit(self, timeout: float = 20) -> int:
        for _ in range(int(timeout / 0.05)):
            code = self.popen.poll()
            if code is not None:
                return code
            await asyncio.sleep(0.05)
        raise AssertionError(f"process did not exit; log:\n{self.output()}")

    def output(self) -> str:
        self._log.flush()
        return self.log_path.read_text()

    def stop(self) -> None:
        if self.popen.poll() is None:
            self.signal(signal.SIGCONT)
            self.signal(signal.SIGKILL)
            self.popen.wait(10)
        self._log.close()


class ProcessGroup:
    def __init__(self, tmp: Path, env: dict[str, str]) -> None:
        self.tmp = tmp
        self.env = env
        self.procs: list[Proc] = []

    def spawn(self, name: str, *args: str, **env_extra: str) -> Proc:
        p = Proc(list(args), {**self.env, **env_extra}, self.tmp / f"{name}.log")
        self.procs.append(p)
        return p

    def worker(self, worker_id: str, concurrency: int = 2, **env_extra: str) -> Proc:
        return self.spawn(
            worker_id,
            "worker",
            "--concurrency",
            str(concurrency),
            "--worker-id",
            worker_id,
            **env_extra,
        )

    def scheduler(self, name: str = "scheduler", **env_extra: str) -> Proc:
        return self.spawn(name, "scheduler", **env_extra)

    def close(self) -> None:
        for p in self.procs:
            p.stop()


@contextlib.contextmanager
def process_group(tmp: Path, env: dict[str, str]) -> Iterator[ProcessGroup]:
    group = ProcessGroup(tmp, env)
    try:
        yield group
    finally:
        group.close()


# PostgreSQL wire protocol: CommandComplete ('C'), int32 length 11, tag "COMMIT\0".
COMMIT_COMPLETE = b"C\x00\x00\x00\x0bCOMMIT\x00"


class TcpProxy:
    """A TCP forwarder that can sever all connections to simulate a database outage.

    It can also simulate a *lost commit acknowledgement*: once armed, the next
    server->client chunk carrying PostgreSQL's CommandComplete("COMMIT") is
    swallowed and that connection is killed. The commit is durable on the
    server, but the client never learns that it succeeded.
    """

    def __init__(self, target_host: str, target_port: int) -> None:
        self.target = (target_host, target_port)
        self.server: asyncio.Server | None = None
        self.port = 0
        self.connections: set[asyncio.StreamWriter] = set()
        self.cut_off = False
        self.drop_commit_ack = False
        self.dropped_commit_acks = 0

    def arm_drop_commit_ack(self) -> None:
        self.drop_commit_ack = True

    async def start(self) -> None:
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", self.port)
        self.port = self.server.sockets[0].getsockname()[1]

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if self.cut_off:
            writer.close()
            return
        try:
            up_reader, up_writer = await asyncio.open_connection(*self.target)
        except OSError:
            writer.close()
            return
        self.connections.update({writer, up_writer})

        async def pipe(
            src: asyncio.StreamReader, dst: asyncio.StreamWriter, *, from_server: bool
        ) -> None:
            tail = b""  # the marker could straddle two reads
            try:
                while data := await src.read(65536):
                    if from_server and self.drop_commit_ack and COMMIT_COMPLETE in tail + data:
                        self.drop_commit_ack = False
                        self.dropped_commit_acks += 1
                        writer.transport.abort()  # client never sees "COMMIT ok"
                        up_writer.transport.abort()
                        return
                    tail = data[-len(COMMIT_COMPLETE) :]
                    dst.write(data)
                    await dst.drain()
            except (ConnectionError, OSError):
                pass
            finally:
                dst.close()

        await asyncio.gather(
            pipe(reader, up_writer, from_server=False),
            pipe(up_reader, writer, from_server=True),
        )
        self.connections.discard(writer)
        self.connections.discard(up_writer)

    def cut(self) -> None:
        self.cut_off = True
        for w in list(self.connections):
            w.transport.abort()
        self.connections.clear()

    def restore(self) -> None:
        self.cut_off = False

    async def close(self) -> None:
        self.cut()
        if self.server:
            self.server.close()
            await self.server.wait_closed()
