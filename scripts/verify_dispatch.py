"""Run after mvn test. Starts isolated JVMs on the demo ports 8080 and 9001."""
import pathlib
import re
import socket
import subprocess
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parents[1]


def request(port, message):
    with socket.create_connection(("localhost", port), timeout=4) as sock:
        sock.sendall((message + "\n").encode())
        return sock.makefile("r").readline().strip()


def wait_for(predicate):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError("Timed out waiting for JVM output")


def main():
    settings = subprocess.run(
        ["java", "-XshowSettings:properties", "-version"],
        capture_output=True, text=True, errors="replace", check=True,
    )
    java_home = re.search(r"^\s*java.home = (.+)$", settings.stderr, re.MULTILINE).group(1).strip()
    java = str(pathlib.Path(java_home) / "bin" / "java")
    # Fail before starting anything if another demo is using these ports.
    for port in (8080, 9001):
        with socket.socket() as probe:
            probe.bind(("localhost", port))
    processes = []
    with tempfile.TemporaryDirectory() as directory:
        logs = []
        try:
            def start(main_class):
                path = pathlib.Path(directory) / (main_class + ".log")
                stream = path.open("w")
                logs.append(stream)
                process = subprocess.Popen(
                    [java, "-cp", str(ROOT / "target/classes"), main_class],
                    stdout=stream, stderr=subprocess.STDOUT,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                )
                processes.append(process)
                return process, lambda: path.read_text()

            _, scheduler_log = start("scheduler.Main")
            wait_for(lambda: "Scheduler started" in scheduler_log())
            assert request(8080, "SUBMIT|missing|1") == "NO_AVAILABLE_WORKER|missing"
            worker, worker_log = start("worker.Worker")
            wait_for(lambda: "listening on" in worker_log())
            assert request(8080, "SUBMIT|invalid|-1") == "INVALID_TASK|invalid"
            assert request(9001, "EXECUTE|invalid|-1") == "ERROR|INVALID_DURATION"

            # Three tasks occupy all execution threads; ten more fill the queue.
            for index in range(13):
                duration = 6000 if index < 3 else 0
                assert request(8080, f"SUBMIT|task-{index}|{duration}") == f"ACCEPTED|task-{index}"
            assert request(8080, "SUBMIT|overflow|0") == "SERVER_BUSY|overflow"
            assert request(8080, "SUBMIT|task-0|0") == "DUPLICATE|task-0"
            wait_for(lambda: worker_log().count(" starts ") >= 3)
            assert worker_log().count(" starts ") == 3
            assert " completed " not in worker_log()
            wait_for(lambda: worker_log().count(" completed ") == 13)
            assert worker_log().count("HEARTBEAT_ACK") >= 3
            assert "suspected dead" not in scheduler_log()
            assert " starts " not in scheduler_log()
            assert request(8080, "SUBMIT|overflow|0") == "ACCEPTED|overflow"
            worker.terminate()
            worker.wait(timeout=5)
            assert request(8080, "SUBMIT|uncertain|1") == "DISPATCH_UNKNOWN|uncertain"
            assert request(8080, "SUBMIT|uncertain|1") == "DUPLICATE|uncertain"
            print("PASS: remote concurrency, queue backpressure, heartbeat, validation, duplicate IDs, dispatch uncertainty")
        finally:
            for process in processes:
                if process.poll() is None:
                    process.terminate()
                    process.wait(timeout=5)
            for stream in logs:
                stream.close()


if __name__ == "__main__":
    main()
