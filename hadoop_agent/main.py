#!/usr/bin/env python3
"""
Hadoop Agent - main.py
Installs Apache Hadoop 3.4.2 on Ubuntu/Linux
Place this at: hadoop_agent/main.py in sherwinsam07/ai_agents repo
"""

import os
import subprocess
import sys
import tarfile
import urllib.request
import shutil
from pathlib import Path

HADOOP_VERSION = os.environ.get("AGENT_VERSION", "3.4.2")
HADOOP_HOME    = os.environ.get("HADOOP_HOME", f"/opt/hadoop-{HADOOP_VERSION}")
INSTALL_DIR    = "/opt"

def run(cmd, shell=True):
    print(f"\n>>> {cmd}")
    result = subprocess.run(cmd, shell=shell, executable="/bin/bash",
                            text=True, capture_output=True)
    if result.stdout.strip():
        print(result.stdout.strip())
    if result.stderr.strip():
        print(result.stderr.strip())
    return result

def step(msg):
    print(f"\n{'='*55}")
    print(f"  {msg}")
    print(f"{'='*55}")

def check_java():
    step("Step 1 — Checking Java")
    r = run("java -version")
    if r.returncode != 0:
        print("Java not found. Installing OpenJDK 11...")
        r = run("sudo apt-get update -qq && sudo apt-get install -y openjdk-11-jdk")
        if r.returncode != 0:
            print("ERROR: Could not install Java. Aborting.")
            sys.exit(1)
    r2 = run("java -version")
    print("Java is ready.")

def download_hadoop():
    step(f"Step 2 — Downloading Hadoop {HADOOP_VERSION}")
    tarball = f"/tmp/hadoop-{HADOOP_VERSION}.tar.gz"

    if os.path.exists(tarball):
        print(f"Already downloaded: {tarball}")
        return tarball

    mirrors = [
        f"https://downloads.apache.org/hadoop/common/hadoop-{HADOOP_VERSION}/hadoop-{HADOOP_VERSION}.tar.gz",
        f"https://archive.apache.org/dist/hadoop/common/hadoop-{HADOOP_VERSION}/hadoop-{HADOOP_VERSION}.tar.gz",
    ]

    for url in mirrors:
        print(f"Trying: {url}")
        try:
            urllib.request.urlretrieve(url, tarball)
            print(f"Downloaded to {tarball}")
            return tarball
        except Exception as e:
            print(f"Failed: {e}")

    print("ERROR: Could not download Hadoop from any mirror.")
    sys.exit(1)

def install_hadoop(tarball):
    step(f"Step 3 — Installing Hadoop to {INSTALL_DIR}")
    target = f"{INSTALL_DIR}/hadoop-{HADOOP_VERSION}"

    if os.path.exists(target):
        print(f"Already exists: {target} — removing old installation...")
        run(f"sudo rm -rf {target}")

    print(f"Extracting {tarball} ...")
    run(f"sudo tar -xzf {tarball} -C {INSTALL_DIR}")

    if os.path.exists(target):
        print(f"Extracted to {target}")
    else:
        print(f"ERROR: Extraction failed. {target} not found.")
        sys.exit(1)

    # Create symlink /opt/hadoop -> /opt/hadoop-3.4.2
    run(f"sudo ln -sfn {target} /opt/hadoop")
    print(f"Symlink created: /opt/hadoop -> {target}")

def configure_env():
    step("Step 4 — Configuring environment variables")

    java_home_r = run("dirname $(dirname $(readlink -f $(which java)))")
    java_home = java_home_r.stdout.strip() or "/usr/lib/jvm/java-11-openjdk-amd64"

    bashrc = Path.home() / ".bashrc"
    block = f"""
# === Hadoop {HADOOP_VERSION} - added by hadoop_agent ===
export JAVA_HOME={java_home}
export HADOOP_HOME=/opt/hadoop
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"
# === End Hadoop ===
"""
    content = bashrc.read_text() if bashrc.exists() else ""
    if "HADOOP_HOME" in content:
        print("Hadoop env vars already in ~/.bashrc — updating...")
        # Remove old block
        lines = content.split("\n")
        new_lines = []
        skip = False
        for line in lines:
            if "=== Hadoop" in line and "added by hadoop_agent" in line:
                skip = True
            if skip and "=== End Hadoop ===" in line:
                skip = False
                continue
            if not skip:
                new_lines.append(line)
        content = "\n".join(new_lines)

    bashrc.write_text(content + block)
    print("Environment variables written to ~/.bashrc")

    # Also set JAVA_HOME in hadoop-env.sh
    hadoop_env = f"/opt/hadoop/etc/hadoop/hadoop-env.sh"
    if os.path.exists(hadoop_env):
        run(f"sudo sed -i 's|# export JAVA_HOME=.*|export JAVA_HOME={java_home}|' {hadoop_env}")
        run(f"grep -q 'export JAVA_HOME={java_home}' {hadoop_env} || echo 'export JAVA_HOME={java_home}' | sudo tee -a {hadoop_env}")
        print(f"JAVA_HOME set in {hadoop_env}")

def configure_pseudo_distributed():
    step("Step 5 — Configuring Hadoop (pseudo-distributed mode)")

    conf_dir = "/opt/hadoop/etc/hadoop"

    # core-site.xml
    core_site = f"""\
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>"""
    run(f"echo '{core_site}' | sudo tee {conf_dir}/core-site.xml")

    # hdfs-site.xml
    hdfs_dir = Path.home() / "hadoop_data"
    namenode_dir = hdfs_dir / "namenode"
    datanode_dir = hdfs_dir / "datanode"
    namenode_dir.mkdir(parents=True, exist_ok=True)
    datanode_dir.mkdir(parents=True, exist_ok=True)

    hdfs_site = f"""\
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file://{namenode_dir}</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file://{datanode_dir}</value>
  </property>
</configuration>"""
    run(f"echo '{hdfs_site}' | sudo tee {conf_dir}/hdfs-site.xml")

    # mapred-site.xml
    mapred_site = """\
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
</configuration>"""
    run(f"echo '{mapred_site}' | sudo tee {conf_dir}/mapred-site.xml")

    # yarn-site.xml
    yarn_site = """\
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
</configuration>"""
    run(f"echo '{yarn_site}' | sudo tee {conf_dir}/yarn-site.xml")

    print("All config files written.")

def setup_ssh():
    step("Step 6 — Setting up passwordless SSH (needed for Hadoop daemons)")
    run("sudo apt-get install -y openssh-server openssh-client")
    ssh_dir = Path.home() / ".ssh"
    ssh_dir.mkdir(exist_ok=True)
    key_path = ssh_dir / "id_rsa"
    if not key_path.exists():
        run(f"ssh-keygen -t rsa -P '' -f {key_path}")
    run(f"cat {ssh_dir}/id_rsa.pub >> {ssh_dir}/authorized_keys")
    run(f"chmod 600 {ssh_dir}/authorized_keys")
    run("sudo service ssh start")
    run("ssh -o StrictHostKeyChecking=no localhost echo 'SSH OK'")
    print("SSH configured.")

def format_namenode():
    step("Step 7 — Formatting HDFS namenode")
    r = run("bash -lc 'source ~/.bashrc && hdfs namenode -format -force'")
    if r.returncode != 0:
        # Try with full path
        r = run(f"sudo /opt/hadoop/bin/hdfs namenode -format -force")
    if r.returncode == 0:
        print("Namenode formatted successfully.")
    else:
        print("WARNING: Namenode format had issues — may already be formatted.")

def start_hadoop():
    step("Step 8 — Starting Hadoop daemons")
    run("bash -lc 'source ~/.bashrc && start-dfs.sh'")
    run("bash -lc 'source ~/.bashrc && start-yarn.sh'")
    import time
    time.sleep(5)

def verify():
    step("Step 9 — Verifying Hadoop is running")
    r = run("bash -lc 'source ~/.bashrc && jps'")
    running = r.stdout

    expected = ["NameNode", "DataNode", "ResourceManager", "NodeManager"]
    all_ok = True
    for proc in expected:
        if proc in running:
            print(f"  OK  {proc} is running")
        else:
            print(f"  MISSING  {proc} is NOT running")
            all_ok = False

    r2 = run("bash -lc 'source ~/.bashrc && hadoop version'")
    print(f"\nHadoop version: {r2.stdout.strip()[:80]}")
    return all_ok

def main():
    print("\n" + "="*55)
    print(f"  HADOOP AGENT — Installing Hadoop {HADOOP_VERSION}")
    print("="*55)

    check_java()
    tarball = download_hadoop()
    install_hadoop(tarball)
    configure_env()
    configure_pseudo_distributed()
    setup_ssh()
    format_namenode()
    start_hadoop()
    ok = verify()

    print("\n" + "="*55)
    if ok:
        print(f"  SUCCESS: Hadoop {HADOOP_VERSION} is installed and running!")
        print(f"\n  Web UIs:")
        print(f"    HDFS NameNode : http://localhost:9870")
        print(f"    YARN          : http://localhost:8088")
        print(f"\n  Commands:")
        print(f"    hadoop version")
        print(f"    hdfs dfs -ls /")
        print(f"    start-dfs.sh / stop-dfs.sh")
        print(f"    start-yarn.sh / stop-yarn.sh")
    else:
        print(f"  Hadoop {HADOOP_VERSION} installed but some daemons not running.")
        print(f"  Check logs in /opt/hadoop/logs/")
    print("="*55 + "\n")

if __name__ == "__main__":
    main()
