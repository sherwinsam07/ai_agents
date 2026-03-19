#!/usr/bin/env python3
"""
mcp_server.py - v3.0
Fixed: handles ALL input() prompts including the reinstall choice prompt.
Automated stdin: port, username, password, AND the repair/reinstall choice.
"""

import sys, json, os, subprocess, tempfile, logging
import urllib.request, urllib.error

logging.basicConfig(
    filename=os.path.expanduser("~/ai_agents_mcp.log"),
    level=logging.DEBUG,
    format="%(asctime)s  %(levelname)s  %(message)s"
)
log = logging.getLogger(__name__)

GITHUB_USER   = "sherwinsam07"
GITHUB_REPO   = "ai_agents"
GITHUB_BRANCH = "main"
GITHUB_TOKEN  = os.environ.get("GITHUB_TOKEN", "")
RAW_BASE = f"https://raw.githubusercontent.com/{GITHUB_USER}/{GITHUB_REPO}/{GITHUB_BRANCH}"

TOOLS = [
    {
        "name": "run_hadoop_agent",
        "description": "Fetches and runs hadoop_agent from github.com/sherwinsam07/ai_agents to install Apache Hadoop. Trigger when user says: install hadoop, setup hadoop.",
        "inputSchema": {"type": "object", "properties": {"version": {"type": "string", "default": "3.3.6"}}}
    },
    {
        "name": "run_spark_agent",
        "description": "Fetches and runs spark_agent from github.com/sherwinsam07/ai_agents to install Apache Spark. Trigger when user says: install spark, setup spark.",
        "inputSchema": {"type": "object", "properties": {"version": {"type": "string", "default": "3.5.0"}}}
    },
    {
        "name": "run_airflow_agent",
        "description": (
            "Fetches and runs airflow_agent from github.com/sherwinsam07/ai_agents "
            "to install or reinstall Apache Airflow. "
            "Trigger when user says: install airflow, reinstall airflow, setup airflow, repair airflow. "
            "Supports action: install (default), verify, repair, reinstall."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "action":   {
                    "type": "string",
                    "description": "What to do: install, verify, repair, reinstall (default: reinstall if already installed)",
                    "default": "reinstall"
                },
                "port":     {"type": "string", "default": "8080"},
                "username": {"type": "string", "default": "admin"},
                "password": {"type": "string", "default": "admin"}
            }
        }
    }
]

AIRFLOW_FILES = [
    "main.py", "config.py", "runner.py", "checks.py",
    "installer.py", "configurator.py", "fixer.py",
    "verifier.py", "ai_analyzer.py"
]
GENERIC_FILES = ["main.py", "agent.py", "install.py", "run.py", "config.py", "runner.py"]


def fetch_file(path):
    url = f"{RAW_BASE}/{path}"
    req = urllib.request.Request(url)
    if GITHUB_TOKEN:
        req.add_header("Authorization", f"token {GITHUB_TOKEN}")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.read().decode("utf-8")
    except Exception as e:
        log.warning(f"Could not fetch {url}: {e}")
        return None


def fetch_folder(agent_folder, tmp_dir):
    files = AIRFLOW_FILES if "airflow" in agent_folder else GENERIC_FILES
    downloaded = []
    for filename in files:
        content = fetch_file(f"{agent_folder}/{filename}")
        if content:
            with open(os.path.join(tmp_dir, filename), "w") as f:
                f.write(content)
            downloaded.append(filename)
    # fetch rules/error_rules.json
    rules = fetch_file(f"{agent_folder}/rules/error_rules.json")
    if rules:
        os.makedirs(os.path.join(tmp_dir, "rules"), exist_ok=True)
        with open(os.path.join(tmp_dir, "rules", "error_rules.json"), "w") as f:
            f.write(rules)
        downloaded.append("rules/error_rules.json")
    return "main.py" in downloaded, downloaded


def fix_paths(tmp_dir):
    """Fix hardcoded ~/airflow_cmd_agent paths in fixer.py."""
    fixer_path = os.path.join(tmp_dir, "fixer.py")
    if os.path.exists(fixer_path):
        with open(fixer_path, "r") as f:
            content = f.read()
        content = content.replace(
            'Path.home() / "airflow_cmd_agent" / "rules" / "error_rules.json"',
            f'Path("{tmp_dir}") / "rules" / "error_rules.json"'
        )
        with open(fixer_path, "w") as f:
            f.write(content)


def run_agent_script(tmp_dir, stdin_input=None, timeout=1200):
    main_path = os.path.join(tmp_dir, "main.py")
    env = os.environ.copy()
    env["PYTHONPATH"] = tmp_dir
    try:
        result = subprocess.run(
            [sys.executable, main_path],
            input=stdin_input,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
            cwd=tmp_dir
        )
        output = result.stdout or ""
        if result.stderr:
            output += f"\n[stderr]:\n{result.stderr}"
        if result.returncode != 0:
            output = f"Agent exited with code {result.returncode}\n{output}"
        return output.strip() or "Agent completed with no output."
    except subprocess.TimeoutExpired:
        return "Agent timed out after the allowed time."
    except Exception as e:
        return f"Error running agent: {e}"


def run_agent(tool_name, args):

    if tool_name == "run_airflow_agent":
        port     = args.get("port", "8080")
        username = args.get("username", "admin")
        password = args.get("password", "admin")
        action   = args.get("action", "reinstall").lower()

        # Map action to the choice number in main.py's prompt:
        # "Choose [1=verify, 2=repair, 3=reinstall]:"
        action_map = {"verify": "1", "repair": "2", "reinstall": "3"}
        choice = action_map.get(action, "3")  # default 3=reinstall

        tmp_dir = tempfile.mkdtemp(prefix="airflow_agent_")
        ok, downloaded = fetch_folder("airflow_agent", tmp_dir)
        log.info(f"Airflow agent downloaded: {downloaded}")

        if not ok:
            return (
                f"Could not download airflow_agent/main.py from GitHub.\n"
                f"Downloaded: {downloaded}"
            )

        fix_paths(tmp_dir)

        # stdin answers ALL input() and getpass() calls in order:
        # 1. "Enter Airflow port [8080]: "          → port
        # 2. "Enter admin username: "               → username
        # 3. "Enter admin password: " (getpass)     → password
        # 4. "Choose [1=verify, 2=repair, 3=reinstall]: " → choice (only shown if already installed)
        # We send extra lines so every possible prompt is answered
        stdin_input = f"{port}\n{username}\n{password}\n{choice}\n{choice}\n"

        log.info(f"Running airflow agent: action={action} choice={choice} port={port} user={username}")
        return run_agent_script(tmp_dir, stdin_input=stdin_input, timeout=1200)

    elif tool_name == "run_hadoop_agent":
        tmp_dir = tempfile.mkdtemp(prefix="hadoop_agent_")
        ok, downloaded = fetch_folder("hadoop_agent", tmp_dir)
        if not ok:
            return f"Could not download hadoop_agent/main.py from GitHub.\nDownloaded: {downloaded}"
        env = os.environ.copy()
        env["AGENT_VERSION"] = args.get("version", "3.3.6")
        env["PYTHONPATH"] = tmp_dir
        try:
            result = subprocess.run(
                [sys.executable, os.path.join(tmp_dir, "main.py")],
                capture_output=True, text=True, timeout=600, env=env, cwd=tmp_dir
            )
            out = result.stdout or ""
            if result.stderr: out += f"\n[stderr]:\n{result.stderr}"
            return out.strip() or "Done."
        except Exception as e:
            return f"Error: {e}"

    elif tool_name == "run_spark_agent":
        tmp_dir = tempfile.mkdtemp(prefix="spark_agent_")
        ok, downloaded = fetch_folder("spark_agent", tmp_dir)
        if not ok:
            return f"Could not download spark_agent/main.py from GitHub.\nDownloaded: {downloaded}"
        env = os.environ.copy()
        env["AGENT_VERSION"] = args.get("version", "3.5.0")
        env["PYTHONPATH"] = tmp_dir
        try:
            result = subprocess.run(
                [sys.executable, os.path.join(tmp_dir, "main.py")],
                capture_output=True, text=True, timeout=600, env=env, cwd=tmp_dir
            )
            out = result.stdout or ""
            if result.stderr: out += f"\n[stderr]:\n{result.stderr}"
            return out.strip() or "Done."
        except Exception as e:
            return f"Error: {e}"

    return f"Unknown tool: {tool_name}"


def handle(req):
    method = req.get("method", "")
    rid    = req.get("id")

    if method == "initialize":
        return {"jsonrpc": "2.0", "id": rid, "result": {
            "protocolVersion": "2024-11-05",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "ai-agents-github-mcp", "version": "3.0.0"}
        }}

    if method == "tools/list":
        return {"jsonrpc": "2.0", "id": rid, "result": {"tools": TOOLS}}

    if method == "tools/call":
        params    = req.get("params", {})
        tool_name = params.get("name", "")
        arguments = params.get("arguments", {})
        log.info(f"Tool call: {tool_name}  args={arguments}")
        output = run_agent(tool_name, arguments)
        return {"jsonrpc": "2.0", "id": rid, "result": {
            "content": [{"type": "text", "text": output}],
            "isError": False
        }}

    if method == "notifications/initialized":
        return None

    return {"jsonrpc": "2.0", "id": rid,
            "error": {"code": -32601, "message": f"Method not found: {method}"}}


def main():
    log.info("ai-agents-github-mcp v3.0 started")
    for raw in sys.stdin:
        raw = raw.strip()
        if not raw:
            continue
        try:
            req  = json.loads(raw)
            resp = handle(req)
            if resp is not None:
                print(json.dumps(resp), flush=True)
        except json.JSONDecodeError as e:
            print(json.dumps({"jsonrpc": "2.0", "id": None,
                "error": {"code": -32700, "message": f"Parse error: {e}"}}), flush=True)
        except Exception as e:
            log.exception("Unhandled error")
            print(json.dumps({"jsonrpc": "2.0", "id": None,
                "error": {"code": -32603, "message": str(e)}}), flush=True)

if __name__ == "__main__":
    main()
