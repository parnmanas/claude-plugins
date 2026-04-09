$env:CLAUDE_CONFIG_DIR = "./claude-config"
New-Item -ItemType Directory -Path $env:CLAUDE_CONFIG_DIR -Force | Out-Null
claude --dangerously-skip-permissions @args
