#!/usr/bin/env python3
"""Deploy a Debian "toolbox" pod (Kubernetes) or container (local Docker),
build librdkafka from a given git ref inside it, and run its tests
(via tests/run-test-batches.py) against an external Kafka cluster.

Example (Kubernetes):

  ./deploy_toolbox_pod.py \\
      --test-conf ~/my-cluster-test.conf \\
      --test-env ~/my-run.env

Example (local Docker):

  ./deploy_toolbox_pod.py --docker \\
      --test-conf ~/my-cluster-test.conf \\
      --test-env ~/my-run.env

By default the pod/container is left running afterwards so it can be
reused (e.g. to tweak --test-env and re-run, or to exec into it for
debugging). Pass --delete to tear it down once the run completes.
"""
import argparse
import datetime
import os
import shlex
import subprocess
import sys

DEFAULT_PACKAGES = [
    "wget",
    "curl",
    "python3",
    "git",
    "openjdk-21-jdk-headless",
    "python3-venv",
    "python3-setuptools",
    "build-essential",
    "libssl-dev",
    "libcurl4-openssl-dev",
    "zlib1g-dev",
    "libsasl2-dev",
    "libzstd-dev",
]

DEFAULT_IMAGE = "debian:trixie"
DEFAULT_GIT_URL = "https://github.com/confluentinc/librdkafka.git"
DEFAULT_GIT_REF = "k2_testing_final"
DEFAULT_REPO_DIR = "/root/librdkafka"
DEFAULT_POD_NAME = "librdkafka-toolbox"


def die(msg, code=1):
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(code)


def run(cmd, **kwargs):
    print(f"+ {' '.join(shlex.quote(c) for c in cmd)}")
    return subprocess.run(cmd, **kwargs)


def stream_exec(cmd, log_file=None):
    """Run cmd, streaming stdout/stderr live and optionally tee-ing to
    log_file. Returns (exit_code, captured_output)."""
    logf = open(log_file, "a") if log_file else None
    captured = []
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT, text=True, bufsize=1)
        for line in proc.stdout:
            sys.stdout.write(line)
            captured.append(line)
            if logf:
                logf.write(line)
        proc.wait()
        return proc.returncode, "".join(captured)
    finally:
        if logf:
            logf.close()


def with_env_exports(script, env):
    """Prefix a shell script with `export KEY=VALUE` lines for env."""
    if not env:
        return script
    exports = "\n".join(f"export {k}={shlex.quote(v)}" for k, v in env.items())
    return exports + "\n" + script


class K8sBackend:
    """Runs commands inside a Kubernetes pod via kubectl."""

    def __init__(self, context, namespace, name, image):
        self.context = context
        self.namespace = namespace
        self.name = name
        self.image = image

    def _base(self):
        cmd = ["kubectl"]
        if self.context:
            cmd += ["--context", self.context]
        if self.namespace:
            cmd += ["-n", self.namespace]
        return cmd

    def _phase(self):
        cmd = self._base() + [
            "get", "pod", self.name, "-o", "jsonpath={.status.phase}",
        ]
        p = subprocess.run(cmd, capture_output=True, text=True)
        if p.returncode != 0:
            return None
        return p.stdout.strip() or None

    def ensure(self):
        phase = self._phase()
        if phase:
            print(f"Pod {self.name!r} already exists (phase={phase}); reusing it.")
        else:
            print(f"Creating pod {self.name!r} (image={self.image}) "
                  f"in context={self.context!r} namespace={self.namespace!r} ...")
            cmd = self._base() + [
                "run", self.name, f"--image={self.image}", "--restart=Never",
                "--command", "--", "sleep", "infinity",
            ]
            if run(cmd).returncode != 0:
                die(f"failed to create pod {self.name}")

        print("Waiting for pod to become Ready ...")
        cmd = self._base() + [
            "wait", "--for=condition=Ready", f"pod/{self.name}", "--timeout=180s",
        ]
        if run(cmd).returncode != 0:
            die(f"pod {self.name} did not become Ready in time")

    def exec(self, script, log_file=None, env=None):
        full_script = with_env_exports(script, env)
        cmd = self._base() + ["exec", "-i", self.name, "--", "bash", "-c", full_script]
        print(f"+ kubectl exec {self.name} -- bash -c '...' (streaming below)")
        return stream_exec(cmd, log_file=log_file)

    def cp(self, src, dest_path):
        cmd = self._base() + ["cp", src, f"{self.name}:{dest_path}"]
        return run(cmd).returncode

    def cleanup(self):
        print(f"\nDeleting pod {self.name} ...")
        cmd = self._base() + ["delete", "pod", self.name, "--ignore-not-found=true"]
        run(cmd)

    def reconnect_hint(self):
        ctx = f"--context {self.context} " if self.context else ""
        ns = f"-n {self.namespace} " if self.namespace else ""
        return (f"Pod left running. To exec into it again:\n"
                f"  kubectl {ctx}{ns}exec -it {self.name} -- bash")


class DockerBackend:
    """Runs commands inside a local Docker container."""

    def __init__(self, name, image, network=None):
        self.name = name
        self.image = image
        self.network = network

    def _status(self):
        cmd = ["docker", "inspect", "-f", "{{.State.Status}}", self.name]
        p = subprocess.run(cmd, capture_output=True, text=True)
        if p.returncode != 0:
            return None
        return p.stdout.strip() or None

    def ensure(self):
        status = self._status()
        if status == "running":
            print(f"Container {self.name!r} already running; reusing it.")
        elif status is not None:
            print(f"Container {self.name!r} exists but is {status!r}; starting it.")
            if run(["docker", "start", self.name]).returncode != 0:
                die(f"failed to start container {self.name}")
        else:
            print(f"Creating container {self.name!r} (image={self.image}) ...")
            cmd = ["docker", "run", "-d", "--name", self.name]
            if self.network:
                cmd += ["--network", self.network]
            cmd += [self.image, "sleep", "infinity"]
            if run(cmd).returncode != 0:
                die(f"failed to create container {self.name}")

    def exec(self, script, log_file=None, env=None):
        full_script = with_env_exports(script, env)
        cmd = ["docker", "exec", "-i", self.name, "bash", "-c", full_script]
        print(f"+ docker exec {self.name} -- bash -c '...' (streaming below)")
        return stream_exec(cmd, log_file=log_file)

    def cp(self, src, dest_path):
        cmd = ["docker", "cp", src, f"{self.name}:{dest_path}"]
        return run(cmd).returncode

    def cleanup(self):
        print(f"\nRemoving container {self.name} ...")
        run(["docker", "rm", "-f", self.name])

    def reconnect_hint(self):
        return (f"Container left running. To exec into it again:\n"
                f"  docker exec -it {self.name} bash")


def current_context():
    p = subprocess.run(["kubectl", "config", "current-context"],
                        capture_output=True, text=True)
    if p.returncode != 0:
        die(f"could not determine current kubectl context: {p.stderr.strip()}")
    return p.stdout.strip()


def current_namespace():
    p = subprocess.run(
        ["kubectl", "config", "view", "--minify", "-o", "jsonpath={..namespace}"],
        capture_output=True, text=True)
    return p.stdout.strip() or "default"


def parse_env_file(path):
    """Parse a simple KEY=VALUE env file (# comments and blank lines ignored)."""
    env = {}
    if not path:
        return env
    with open(path) as f:
        for lineno, raw in enumerate(f, 1):
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                die(f"{path}:{lineno}: invalid line (expected KEY=VALUE): {line!r}")
            k, _, v = line.partition("=")
            k, v = k.strip(), v.strip()
            if len(v) >= 2 and v[0] == v[-1] and v[0] in "\"'":
                v = v[1:-1]
            env[k] = v
    return env


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--docker", action="store_true",
                     help="use a local Docker container instead of a Kubernetes pod")
    ap.add_argument("--docker-network", default="host",
                     help="Docker network to attach the container to; "
                          "defaults to 'host' so 'localhost:9092' in "
                          "test.conf reaches brokers exposed on the Docker "
                          "host. Pass '' to use Docker's default bridge "
                          "network instead. Only used with --docker")
    ap.add_argument("--context", default=None,
                     help="kubectl context (default: current context; ignored with --docker)")
    ap.add_argument("--namespace", "-n", default=None,
                     help="k8s namespace (default: current namespace; ignored with --docker)")
    ap.add_argument("--pod-name", default=DEFAULT_POD_NAME,
                     help="toolbox pod/container name")
    ap.add_argument("--image", default=DEFAULT_IMAGE,
                     help="base container image")
    ap.add_argument("--git-url", default=DEFAULT_GIT_URL,
                     help="librdkafka git repo URL")
    ap.add_argument("--git-ref", default=DEFAULT_GIT_REF,
                     help="git branch/tag/commit to check out")
    ap.add_argument("--repo-dir", default=DEFAULT_REPO_DIR,
                     help="path inside the pod/container to clone into")
    ap.add_argument("--packages", nargs="+", default=DEFAULT_PACKAGES,
                     help="apt packages to install")
    ap.add_argument("--test-conf", required=True,
                     help="local test.conf file, copied to <repo>/tests/test.conf")
    ap.add_argument("--test-env", default=None,
                     help="local KEY=VALUE file exported before running test-runner")
    ap.add_argument("--delete", action="store_true",
                     help="delete the pod/container after the run completes")
    ap.add_argument("--skip-install", action="store_true",
                     help="skip apt-get install step (already provisioned)")
    ap.add_argument("--skip-build", action="store_true",
                     help="skip git clone/checkout + configure/make step")
    ap.add_argument("--force-build", action="store_true",
                     help="run configure/make even if the git pull fetched "
                          "no new commits (by default it's skipped when "
                          "already up to date and test-runner exists)")
    ap.add_argument("--log-dir", default="./toolbox-logs",
                     help="local directory to store run logs")
    args = ap.parse_args()

    if not os.path.isfile(args.test_conf):
        die(f"--test-conf file not found: {args.test_conf}")
    if args.test_env and not os.path.isfile(args.test_env):
        die(f"--test-env file not found: {args.test_env}")

    os.makedirs(args.log_dir, exist_ok=True)
    ts = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    log_file = os.path.join(args.log_dir, f"run-{ts}.log")

    if args.docker:
        print(f"Backend:   docker")
        print(f"Container: {args.pod_name}")
        backend = DockerBackend(args.pod_name, args.image, network=args.docker_network)
    else:
        context = args.context or current_context()
        namespace = args.namespace or current_namespace()
        print(f"Backend:   kubernetes")
        print(f"Context:   {context}")
        print(f"Namespace: {namespace}")
        print(f"Pod:       {args.pod_name}")
        backend = K8sBackend(context, namespace, args.pod_name, args.image)

    print(f"Log file:  {log_file}")

    backend.ensure()

    if not args.skip_install:
        print("\n== Installing dependencies ==")
        pkgs = " ".join(shlex.quote(p) for p in args.packages)
        script = f"""set -e
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y {pkgs}
"""
        rc, _ = backend.exec(script, log_file=log_file)
        if rc != 0:
            die("dependency installation failed")

    if not args.skip_build:
        print("\n== Cloning / checking out librdkafka ==")
        repo_dir_q = shlex.quote(args.repo_dir)
        ref_q = shlex.quote(args.git_ref)
        # Prints a REBUILD_NEEDED=yes/no marker line: rebuild is needed on a
        # fresh clone, whenever the pulled ref's commit actually changed, or
        # if a previous build never completed (test-runner missing).
        script = f"""set -e
if [ -d {repo_dir_q}/.git ]; then
    cd {repo_dir_q}
    OLD_HEAD=$(git rev-parse HEAD)
    git fetch origin {ref_q}
    git checkout {ref_q}
    git reset --hard origin/{ref_q}
    NEW_HEAD=$(git rev-parse HEAD)
    if [ "$OLD_HEAD" = "$NEW_HEAD" ] && [ -x tests/test-runner ]; then
        echo "REBUILD_NEEDED=no"
    else
        echo "REBUILD_NEEDED=yes"
    fi
else
    git clone --branch {ref_q} --single-branch {shlex.quote(args.git_url)} {repo_dir_q}
    echo "REBUILD_NEEDED=yes"
fi
"""
        rc, out = backend.exec(script, log_file=log_file)
        if rc != 0:
            die("git clone/checkout failed")
        rebuild_needed = args.force_build or "REBUILD_NEEDED=no" not in out

        if rebuild_needed:
            print("\n== Configuring and building librdkafka ==")
            # 'make libs' builds src+src-cpp only, skipping the top-level 'all'
            # target's CONFIGURATION.md/check/TAGS steps which need extra tools
            # (e.g. ctags) not in our dependency list.
            script = f"""set -e
cd {repo_dir_q}
./configure
make -j$(nproc) libs
make -j$(nproc) -C tests build
"""
            rc, _ = backend.exec(script, log_file=log_file)
            if rc != 0:
                die("build failed")
        else:
            print("\n== No new commits pulled; skipping configure/make "
                  "(pass --force-build to rebuild anyway) ==")

    print("\n== Copying test.conf into pod/container ==")
    if backend.cp(args.test_conf, f"{args.repo_dir}/tests/test.conf") != 0:
        die("failed to copy test.conf")

    test_env = parse_env_file(args.test_env)
    test_env.setdefault("RDKAFKA_TEST_CONF", "./test.conf")
    ld_path = f"{args.repo_dir}/src:{args.repo_dir}/src-cpp"
    if "LD_LIBRARY_PATH" in test_env:
        ld_path += ":" + test_env["LD_LIBRARY_PATH"]
    test_env["LD_LIBRARY_PATH"] = ld_path

    print("\n== Running run-test-batches.py ==")
    print("Environment:")
    for k, v in test_env.items():
        print(f"  {k}={v}")

    script = f"cd {shlex.quote(args.repo_dir)}/tests && python3 ./run-test-batches.py"
    rc, _ = backend.exec(script, log_file=log_file, env=test_env)

    print(f"\nrun-test-batches.py exited with code {rc}")
    print(f"Full log saved to: {log_file}")

    if args.delete:
        backend.cleanup()
    else:
        print(f"\n{backend.reconnect_hint()}")

    sys.exit(rc)


if __name__ == "__main__":
    main()
