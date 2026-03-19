#!/usr/bin/env python3
"""
mcp_server.py
ONE single MCP server for Claude Desktop.
When you say "install airflow" it fetches the agent from
github.com/sherwinsam07/ai_agents and runs it locally.
"""

import sys, json, os, subprocess, tempfile, stat, logging
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

AGENT_MAP = {
    "run_hadoop_agent":  "hadoop_agent",
    "run_spark_agent":   "spark_agent",
    "run_airflow_agent": "airflow_agent",
}

RAW_BASE = f"https://raw.githubusercontent.com/{GITHUB_USER}/{GITHUB_REPO}/{GITHUB_BRANCH}"

TOOLS = [
    {
        "name": "run_hadoop_agent",
        "description": (
            "Fetches and runs the hadoop_agent from github.com/sherwinsam07/ai_agents "
            "to install Apache Hadoop on this machine. "
            "Trigger when user says: install hadoop, setup hadoop, configure hadoop."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "version": {
                    "type": "string",
                    "description": "Hadoop version (default: 3.3.6)",
                    "default": "3.3.6"
                }
            }
        }
    },
    {
        "name": "run_spark_agent",
        "description": (
            "Fetches and runs the spark_agent from github.com/sherwinsam07/ai_agents "
            "to install Apache Spark on this machine. "
            "Trigger when user says: install spark, setup spark, configure spark."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "version": {
                    "type": "string",
                    "description": "Spark version (default: 3.5.0)",
                    "default": "3.5.0"
                }
            }
        }
    },
    {
        "name": "run_airflow_agent",
        "description": (
            "Fetches and runs the airflow_agent from github.com/sherwinsam07/ai_agents "
            "to install Apache Airflow on this machine. "
            "Trigger when user says: install airflow, setup airflow, configure airflow."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "version": {
                    "type": "string",
                    "description": "Airflow version (default: 2.8.0)",
                    "default": "2.8.0"
                }
            }
        }
    }
]


def find_agent_script(agent_folder):
    """Try common entry point filenames inside the agent folder."""
    candidates = [
        f"{agent_folder}.py",
        "main.py",
        "agent.py",
        "install.py",
        "run.py",
    ]
    for name in candidates:
        url = f"{RAW_BASE}/{agent_folder}/{name}"
        req = urllib.request.Request(url)
        if GITHUB_TOKEN:
            req.add_header("Authorization", f"token {GITHUB_TOKEN}")
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                content = resp.read().decode("utf-8")
                log.info(f"Found agent script: {agent_folder}/{name}")
                return content, name
        except urllib.error.HTTPError:
            continue
        except Exception:
            continue
    return None, None


def run_agent(tool_name, args):
    agent_folder = AGENT_MAP.get(tool_name)
    if not agent_folder:
        return f"Unknown tool: {tool_name}"

    log.info(f"Looking for agent in folder: {agent_folder}")
    script_content, script_name = find_agent_script(agent_folder)

    if script_content is None:
        return (
            f"Could not find a runnable script inside '{agent_folder}/' on GitHub.\n"
            f"Tried: {agent_folder}.py, main.py, agent.py, install.py, run.py\n"
            f"Please make sure one of these files exists inside the {agent_folder}/ folder."
        )

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".py", delete=False, prefix=f"{tool_name}_"
    ) as f:
        f.write(script_content)
        tmp_path = f.name
    os.chmod(tmp_path, stat.S_IRWXU)

    env = os.environ.copy()
    for k, v in args.items():
        env[f"AGENT_{k.upper()}"] = str(v)

    log.info(f"Running {agent_folder}/{script_name} as {tmp_path}")

    try:
        result = subprocess.run(
            [sys.executable, tmp_path],
            capture_output=True,
            text=True,
            timeout=600,
            env=env
        )
    except subprocess.TimeoutExpired:
        os.unlink(tmp_path)
        return "Agent timed out after 10 minutes."
    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass

    output = result.stdout or ""
    if result.stderr:
        output += f"\n[stderr]:\n{result.stderr}"
    if result.returncode != 0:
        output = f"Agent exited with code {result.returncode}\n{output}"

    return output.strip() or "Agent completed with no output."


def handle(req):
    method = req.get("method", "")
    rid = req.get("id")

    if method == "initialize":
        return {
            "jsonrpc": "2.0", "id": rid,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "ai-agents-github-mcp", "version": "1.0.0"}
            }
        }

    if method == "tools/list":
        return {"jsonrpc": "2.0", "id": rid, "result": {"tools": TOOLS}}

    if method == "tools/call":
        params = req.get("params", {})
        tool_name = params.get("name", "")
        arguments = params.get("arguments", {})
        log.info(f"Tool call: {tool_name}  args={arguments}")
        output = run_agent(tool_name, arguments)
        return {
            "jsonrpc": "2.0", "id": rid,
            "result": {
                "content": [{"type": "text", "text": output}],
                "isError": False
            }
        }

    if method == "notifications/initialized":
        return None

    return {
        "jsonrpc": "2.0", "id": rid,
        "error": {"code": -32601, "message": f"Method not found: {method}"}
    }


def main():
    log.info("ai-agents-github-mcp server started")
    for raw in sys.stdin:
        raw = raw.strip()
        if not raw:
            continue
        try:
            req = json.loads(raw)
            log.debug(f"<- {req}")
            resp = handle(req)
            if resp is not None:
                out = json.dumps(resp)
                log.debug(f"-> {out}")
                print(out, flush=True)
        except json.JSONDecodeError as e:
            print(json.dumps({"jsonrpc": "2.0", "id": None,
                "error": {"code": -32700, "message": f"Parse error: {e}"}}), flush=True)
        except Exception as e:
            log.exception("Unhandled error")
            print(json.dumps({"jsonrpc": "2.0", "id": None,
                "error": {"code": -32603, "message": str(e)}}), flush=True)


if __name__ == "__main__":
    main()
