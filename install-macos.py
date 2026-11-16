#!/usr/bin/env python3
"""Optional macOS service installer; client registration is a separate opt-in.

Only writes WhatsAppMCP's application directory, its LaunchAgent, and the named
Codex MCP entry. It never deletes message/session data or unrelated config.
"""
import argparse
import os
import pathlib
import json
import shutil
import subprocess
import sys
import tomllib

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--register-codex', action='store_true', help='also register this server in Codex (off by default)')
parser.add_argument('--dry-run', action='store_true', help='validate and show the installation plan without changing files or services')
options = parser.parse_args()

if sys.platform != "darwin":
    raise SystemExit("This installer is for macOS. Run the binary manually on other Unix systems.")

source = pathlib.Path(__file__).resolve().parent / "bin" / "whatsapp-mcp"
if not source.is_file():
    raise SystemExit("Build first: make build")
home = pathlib.Path.home()
app = home / "Library" / "Application Support" / "WhatsAppMCP"
binary = app / "bin" / "whatsapp-mcp"
agent = home / "Library" / "LaunchAgents" / "local.whatsapp-mcp.plist"
label = "local.whatsapp-mcp"
domain = f"gui/{os.getuid()}"
codex = shutil.which("codex") if options.register_codex else None
if options.register_codex and not codex:
    raise SystemExit("Codex CLI was not found on PATH.")

config = home / ".codex" / "config.toml"
if options.register_codex and config.exists():
    existing = tomllib.loads(config.read_text()).get("mcp_servers", {}).get("whatsapp")
    if existing and existing.get("command") != str(binary):
        raise SystemExit("An unrelated Codex MCP named whatsapp already exists; refusing to replace it.")
if agent.exists():
    existing = json.loads(subprocess.check_output(["plutil", "-convert", "json", "-o", "-", str(agent)]))
    if existing.get("Label") != label or existing.get("ProgramArguments", [None])[0] != str(binary):
        raise SystemExit("An unrelated LaunchAgent occupies this path; refusing to replace it.")
for path in (app, app / "bin", binary, agent):
    if path.is_symlink():
        raise SystemExit(f"Refusing symlink: {path}")

if options.dry_run:
    print(json.dumps({'source': str(source), 'binary': str(binary), 'launch_agent': str(agent), 'register_codex': options.register_codex}, indent=2))
    raise SystemExit(0)

app.mkdir(parents=True, exist_ok=True, mode=0o700)
app.chmod(0o700)
binary.parent.mkdir(exist_ok=True, mode=0o700)
staged = binary.with_name("whatsapp-mcp.new")
shutil.copyfile(source, staged)
staged.chmod(0o700)
os.replace(staged, binary)
agent.parent.mkdir(parents=True, exist_ok=True)
definition = {
    "Label": label,
    "ProgramArguments": [str(binary), "serve", "--data-dir", str(app)],
    "RunAtLoad": True,
    "KeepAlive": True,
    "ThrottleInterval": 15,
    "ProcessType": "Background",
    "WorkingDirectory": str(app),
    "StandardOutPath": str(app / "service.log"),
    "StandardErrorPath": str(app / "service.log"),
    "Umask": 0o077,
}
if agent.exists():
    subprocess.run(["launchctl", "bootout", f"{domain}/{label}"], capture_output=True)
agent.write_text(json.dumps(definition))
subprocess.run(["plutil", "-convert", "xml1", str(agent)], check=True)
agent.chmod(0o600)
subprocess.run(["launchctl", "bootstrap", domain, str(agent)], check=True)
if options.register_codex:
    subprocess.run([codex, "mcp", "add", "whatsapp", "--", str(binary), "mcp", "--data-dir", str(app)], check=True)
    print("Registered Codex MCP: whatsapp")
print("Installed local WhatsApp service.")
print(f"Pair: {binary} pair --open")
print(f"Status: {binary} status")
