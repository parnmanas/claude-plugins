#!/usr/bin/env node
/**
 * Plugin: claude-self
 *
 * Claude 프로세스 정보 조회 및 라이프사이클 제어.
 * - inspect: 프로세스 정보 조회
 * - kill: SIGTERM으로 종료 (claude-loop.sh에서 --continue로 재시작)
 * - clear: SIGUSR1로 종료 (claude-loop.sh에서 새 세션으로 재시작)
 */

import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import fs from 'fs';

function log(...args) {
  console.error('[claude-self]', ...args);
}

function findClaudePid() {
  let pid = process.ppid;

  for (let i = 0; i < 5; i++) {
    try {
      const cmdline = fs.readFileSync(`/proc/${pid}/cmdline`, 'utf-8');
      if (cmdline.includes('claude')) {
        return pid;
      }
      const status = fs.readFileSync(`/proc/${pid}/status`, 'utf-8');
      const ppidMatch = status.match(/PPid:\s*(\d+)/);
      if (!ppidMatch) break;
      pid = parseInt(ppidMatch[1], 10);
      if (pid <= 1) break;
    } catch {
      break;
    }
  }

  return process.ppid;
}

function readProcInfo(pid) {
  const procDir = `/proc/${pid}`;

  const cmdlineRaw = fs.readFileSync(`${procDir}/cmdline`);
  const argv = [];
  let start = 0;
  for (let i = 0; i < cmdlineRaw.length; i++) {
    if (cmdlineRaw[i] === 0) {
      const arg = cmdlineRaw.subarray(start, i).toString('utf-8');
      if (arg.length > 0) argv.push(arg);
      start = i + 1;
    }
  }
  if (start < cmdlineRaw.length) {
    const arg = cmdlineRaw.subarray(start).toString('utf-8');
    if (arg.length > 0) argv.push(arg);
  }

  const cwd = fs.readlinkSync(`${procDir}/cwd`);

  let exe;
  try {
    exe = fs.readlinkSync(`${procDir}/exe`);
  } catch {
    exe = argv[0];
  }

  return { pid, argv, cwd, exe };
}

// ── MCP Server ──

const server = new McpServer({
  name: 'claude-self',
  version: '2.0.0',
});

server.tool(
  'inspect',
  'Show the current Claude process info: PID, command line, working directory.',
  {},
  async () => {
    try {
      const claudePid = findClaudePid();
      const info = readProcInfo(claudePid);

      const lines = [
        `PID: ${info.pid}`,
        `Exe: ${info.exe}`,
        `CWD: ${info.cwd}`,
        `Argv: ${JSON.stringify(info.argv)}`,
      ];

      return { content: [{ type: 'text', text: lines.join('\n') }] };
    } catch (err) {
      return { content: [{ type: 'text', text: `ERROR: ${err.message}` }] };
    }
  }
);

server.tool(
  'kill',
  'Kill the current Claude process. If running under claude-loop.sh, it will auto-restart with --continue.',
  {},
  async () => {
    try {
      const claudePid = findClaudePid();
      log(`Killing Claude PID ${claudePid}`);

      process.kill(claudePid, 'SIGTERM');

      return { content: [{ type: 'text', text: `Sent SIGTERM to Claude PID ${claudePid}.` }] };
    } catch (err) {
      return { content: [{ type: 'text', text: `ERROR: ${err.message}` }] };
    }
  }
);

server.tool(
  'clear',
  'Kill the current Claude process and restart with a fresh session (no --continue). Clears conversation context.',
  {},
  async () => {
    try {
      const claudePid = findClaudePid();
      log(`Sending SIGUSR1 to Claude PID ${claudePid} for fresh restart`);

      process.kill(claudePid, 'SIGUSR1');

      return { content: [{ type: 'text', text: `Clearing session. Sent SIGUSR1 to Claude PID ${claudePid}.` }] };
    } catch (err) {
      return { content: [{ type: 'text', text: `ERROR: ${err.message}` }] };
    }
  }
);

async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  log(`MCP server running (ppid: ${process.ppid})`);
}

main().catch((err) => {
  log('Fatal:', err);
  process.exit(1);
});
