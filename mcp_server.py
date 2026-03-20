#!/usr/bin/env python3
import sys, json, os, subprocess, tempfile, logging
import urllib.request, urllib.error

logging.basicConfig(filename=os.path.expanduser("~/ai_agents_mcp.log"), level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

GITHUB_USER = "sherwinsam07"
GITHUB_REPO = "ai_agents"
GITHUB_BRANCH = "main"
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
RAW_BASE = f"https://raw.githubusercontent.com/{GITHUB_USER}/{GITHUB_REPO}/{GITHUB_BRANCH}"

TOOLS = [
    {"name": "run_hadoop_agent", "description": "Installs Apache Hadoop 3.4.2. Fetches hadoop_agent/main.py from github.com/sherwinsam07/ai_agents and runs it automatically. No prompts needed. Trigger: install hadoop, setup hadoop, configure hadoop.", "inputSchema": {"type": "object", "properties": {}}},
    {"name": "run_spark_agent", "description": "Installs Apache Spark. Fetches spark_agent/main.py from github.com/sherwinsam07/ai_agents. Trigger: install spark, setup spark.", "inputSchema": {"type": "object", "properties": {"version": {"type": "string", "default": "3.5.0"}}}},
    {"name": "run_airflow_agent", "description": "Installs Apache Airflow. Fetches all airflow_agent files from github.com/sherwinsam07/ai_agents. BEFORE calling ask user for: 1) port 2) username 3) password. Trigger: install airflow, reinstall airflow, repair airflow.", "inputSchema": {"type": "object", "properties": {"action": {"type": "string", "default": "reinstall"}, "port": {"type": "string", "default": "8080"}, "username": {"type": "string", "default": "admin"}, "password": {"type": "string", "default": "admin"}}}}
]

AIRFLOW_FILES = ["main.py","config.py","runner.py","checks.py","installer.py","configurator.py","fixer.py","verifier.py","ai_analyzer.py"]

def fetch_file(path):
    url = f"{RAW_BASE}/{path}"
    req = urllib.request.Request(url)
    if GITHUB_TOKEN:
        req.add_header("Authorization", f"token {GITHUB_TOKEN}")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            content = resp.read().decode("utf-8")
            log.info(f"Fetched {url} ({len(content)} bytes)")
            return content
    except Exception as e:
        log.warning(f"Could not fetch {url}: {e}")
        return None

def fetch_and_run(agent_folder, files, tmp_dir, stdin_input=None, extra_env=None, timeout=900):
    downloaded = []
    for filename in files:
        content = fetch_file(f"{agent_folder}/{filename}")
        if content:
            with open(os.path.join(tmp_dir, filename), "w") as f:
                f.write(content)
            downloaded.append(filename)
    rules = fetch_file(f"{agent_folder}/rules/error_rules.json")
    if rules:
        os.makedirs(os.path.join(tmp_dir, "rules"), exist_ok=True)
        with open(os.path.join(tmp_dir, "rules", "error_rules.json"), "w") as f:
            f.write(rules)
        downloaded.append("rules/error_rules.json")
    log.info(f"{agent_folder} downloaded: {downloaded}")
    if "main.py" not in downloaded:
        return f"ERROR: Could not download {agent_folder}/main.py from GitHub. URL: {RAW_BASE}/{agent_folder}/main.py"
    fixer_path = os.path.join(tmp_dir, "fixer.py")
    if os.path.exists(fixer_path):
        with open(fixer_path, "r") as f:
            fc = f.read()
        fc = fc.replace("Path.home() / \"airflow_cmd_agent\" / \"rules\" / \"error_rules.json\"", f"Path(\"{tmp_dir}\") / \"rules\" / \"error_rules.json\"")
        with open(fixer_path, "w") as f:
            f.write(fc)
    env = os.environ.copy()
    env["PYTHONPATH"] = tmp_dir
    if extra_env:
        env.update(extra_env)
    main_path = os.path.join(tmp_dir, "main.py")
    log.info(f"Running {agent_folder}/main.py")
    try:
        result = subprocess.run([sys.executable, main_path], input=stdin_input, capture_output=True, text=True, timeout=timeout, env=env, cwd=tmp_dir)
        output = result.stdout or ""
        if result.stderr:
            output += f"\n[stderr]:\n{result.stderr}"
        if result.returncode != 0:
            output = f"Agent exited with code {result.returncode}\n{output}"
        return output.strip() or "Agent completed with no output."
    except subprocess.TimeoutExpired:
        return f"Agent timed out after {timeout}s."
    except Exception as e:
        return f"Error: {e}"

def run_agent(tool_name, args):
    if tool_name == "run_hadoop_agent":
        tmp_dir = tempfile.mkdtemp(prefix="hadoop_agent_")
        return fetch_and_run("hadoop_agent", ["main.py"], tmp_dir, stdin_input=None, extra_env={"AGENT_VERSION": "3.4.2"}, timeout=900)
    elif tool_name == "run_spark_agent":
        tmp_dir = tempfile.mkdtemp(prefix="spark_agent_")
        return fetch_and_run("spark_agent", ["main.py"], tmp_dir, stdin_input=None, extra_env={"AGENT_VERSION": args.get("version","3.5.0")}, timeout=600)
    elif tool_name == "run_airflow_agent":
        port = args.get("port","8080")
        username = args.get("username","admin")
        password = args.get("password","admin")
        action = args.get("action","reinstall").lower()
        choice = {"verify":"1","repair":"2","reinstall":"3"}.get(action,"3")
        stdin_input = f"{port}\n{username}\n{password}\n{choice}\n{choice}\n"
        tmp_dir = tempfile.mkdtemp(prefix="airflow_agent_")
        log.info(f"Airflow: action={action} port={port} user={username}")
        return fetch_and_run("airflow_agent", AIRFLOW_FILES, tmp_dir, stdin_input=stdin_input, extra_env=None, timeout=1200)
    return f"Unknown tool: {tool_name}"

def handle(req):
    method = req.get("method","")
    rid = req.get("id")
    if method == "initialize":
        return {"jsonrpc":"2.0","id":rid,"result":{"protocolVersion":"2024-11-05","capabilities":{"tools":{}},"serverInfo":{"name":"ai-agents-github-mcp","version":"6.0.0"}}}
    if method == "tools/list":
        return {"jsonrpc":"2.0","id":rid,"result":{"tools":TOOLS}}
    if method == "tools/call":
        params = req.get("params",{})
        tool_name = params.get("name","")
        arguments = params.get("arguments",{})
        log.info(f"Tool call: {tool_name} args={arguments}")
        output = run_agent(tool_name, arguments)
        return {"jsonrpc":"2.0","id":rid,"result":{"content":[{"type":"text","text":output}],"isError":False}}
    if method == "notifications/initialized":
        return None
    return {"jsonrpc":"2.0","id":rid,"error":{"code":-32601,"message":f"Method not found: {method}"}}

def main():
    log.info("ai-agents-github-mcp v6.0 started")
    for raw in sys.stdin:
        raw = raw.strip()
        if not raw:
            continue
        try:
            req = json.loads(raw)
            resp = handle(req)
            if resp is not None:
                print(json.dumps(resp), flush=True)
        except json.JSONDecodeError as e:
            print(json.dumps({"jsonrpc":"2.0","id":None,"error":{"code":-32700,"message":f"Parse error: {e}"}}), flush=True)
        except Exception as e:
            log.exception("Unhandled error")
            print(json.dumps({"jsonrpc":"2.0","id":None,"error":{"code":-32603,"message":str(e)}}), flush=True)

if __name__ == "__main__":
    main()
