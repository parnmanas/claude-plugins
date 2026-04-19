// ─── Ticket Poller (v0.25.0 polling-based allocation) ─────
//
// v0.25.0 replaces the old busy/idle split (get_pending_triggers +
// get_my_actionable_tickets + manual_trigger) with a single 5-minute call
// to get_allocated_tickets. The server side no longer persists triggers —
// delivery is pure fire-and-forget SSE — so this poll is both the backstop
// for missed SSE events AND the supervisor that detects silent subagents.
//
// Each tick:
//   1. get_allocated_tickets → server returns every (ticket, role) pair
//      where the agent holds a role in a non-terminal column, with a
//      my_last_update_at timestamp per ticket (MAX of the agent's latest
//      comment + latest activity-log row).
//   2. Sort by [column_position DESC, priority_index ASC] so late-stage
//      high-priority tickets drain before fresh backlog items start.
//   3. For each row:
//      a. No live session → spawn one, seeded with the trigger prompt and
//         explicit instructions to post a progress comment every 20 min.
//      b. Live session + my_last_update_at older than SESSION_SILENCE_WARN_MS
//         → send a "30-min silence" follow-up turn (once). If the NEXT tick
//         still shows silence past the threshold, kill the session so the
//         following tick respawns a fresh one (escalation).
//      c. Live session + last user turn > PROGRESS_PROMPT_INTERVAL_MS ago
//         (independent of silence) → send a "post a progress comment now"
//         follow-up so work-in-progress tickets keep the comment stream
//         flowing. Uses session._lastUserTurnAt from the session manager.
//
// Every session-state fields touched here (_warnedSilenceAt,
// _lastProgressPromptAt, _lastUserTurnAt) live on the session record
// returned by _getSession(). They're reset implicitly when the session
// exits and a fresh one is spawned.

import {
  TRIGGER_POLL_INTERVAL_MS,
  SESSION_SILENCE_WARN_MS,
  PROGRESS_PROMPT_INTERVAL_MS,
} from './constants.mjs';
import { log } from './logging.mjs';
import { callMcpTool, unwrapToolResult } from './mcp-client.mjs';
import { fetchTicketContext } from './rest.mjs';
import { loadAgentInfo } from './config.mjs';

export class TicketPoller {
  #config;
  #agentId;
  #workspaceId;
  #ticketSessionManager;
  #timer = null;
  #stopped = false;
  #busy = false;

  constructor(config, ticketSessionManager) {
    this.#config = config;
    this.#ticketSessionManager = ticketSessionManager;
    const info = loadAgentInfo();
    this.#agentId = info?.agent_id || '';
    this.#workspaceId = info?.workspace_id || config?.workspace_id || '';
  }

  async start() {
    if (!this.#agentId) {
      log('Ticket poller skipped — agent_id not in agent.json (run /ai-workflow-board:setup)');
      return;
    }
    if (!this.#ticketSessionManager) {
      log('Ticket poller skipped — ticketSessionManager not provided');
      return;
    }
    if (!this.#workspaceId) {
      this.#workspaceId = await this.#resolveWorkspaceId();
      if (!this.#workspaceId) {
        log('Ticket poller skipped — workspace_id resolve via whoami failed');
        return;
      }
    }
    this.#stopped = false;
    // Deferred initial tick. The proxy runs under ralf-style harnesses that
    // spawn a short-lived claude.exe every 5 seconds just to do tools/list —
    // if we tick on start, every one of those 3-second proxies would dispatch
    // before stdin closes. Waiting one full interval means only the genuinely
    // long-lived proxy (the one backing a real Claude session) fires the poll.
    this.#timer = setInterval(() => {
      this.#tick().catch((err) => log(`Ticket poll failed: ${err.message}`));
    }, TRIGGER_POLL_INTERVAL_MS);
    this.#timer.unref?.();
    log(`Ticket poller started (agent=${this.#agentId.slice(0, 8)} workspace=${this.#workspaceId.slice(0, 8)} interval=${TRIGGER_POLL_INTERVAL_MS / 1000}s, initial tick deferred)`);
  }

  async #resolveWorkspaceId() {
    try {
      const rpc = await callMcpTool(this.#config, 'whoami', {}, { clientName: 'awb-ticket-poller-init' });
      const result = unwrapToolResult(rpc);
      const ws = result?.workspace_id || result?.agent?.workspace_id || '';
      return typeof ws === 'string' ? ws : '';
    } catch (err) {
      log(`whoami for poller workspace_id failed: ${err.message}`);
      return '';
    }
  }

  stop() {
    this.#stopped = true;
    if (this.#timer) {
      clearInterval(this.#timer);
      this.#timer = null;
    }
  }

  async #tick() {
    if (this.#stopped) return;
    if (this.#busy) return;
    this.#busy = true;
    try {
      await this.#runAllocationPass();
    } finally {
      this.#busy = false;
    }
  }

  async #runAllocationPass() {
    const rpc = await callMcpTool(this.#config, 'get_allocated_tickets', {
      agent_id: this.#agentId,
      workspace_id: this.#workspaceId,
    }, { clientName: 'awb-ticket-poller' });
    const result = unwrapToolResult(rpc);
    if (result?.error) {
      log(`Ticket poll: get_allocated_tickets error: ${result.error}`);
      return;
    }
    const rowsRaw = Array.isArray(result) ? result : (result?.data || []);
    if (!Array.isArray(rowsRaw) || rowsRaw.length === 0) return;

    // Sort: late-stage columns drain first; within a column, high priority first.
    // priority_index is authoritative (server computed it from the same list the
    // plugin would use locally — ['critical','high','medium','low']).
    const rows = [...rowsRaw].sort((a, b) => {
      const colDelta = (b.column_position ?? 0) - (a.column_position ?? 0);
      if (colDelta !== 0) return colDelta;
      return (a.priority_index ?? 99) - (b.priority_index ?? 99);
    });

    const now = Date.now();
    log(`Ticket poll: ${rows.length} allocated ticket(s)`);

    for (const row of rows) {
      if (this.#stopped) return;
      const ticketId = row.ticket_id;
      if (!ticketId) continue;

      try {
        await this.#handleAllocatedTicket(row, now);
      } catch (err) {
        log(`Ticket poll handler failed: ticket=${ticketId} err=${err.message}`);
      }
    }
  }

  async #handleAllocatedTicket(row, nowMs) {
    const ticketId = row.ticket_id;
    const role = row.role;
    const lastUpdateAt = row.my_last_update_at ? Date.parse(row.my_last_update_at) : 0;
    const silenceMs = lastUpdateAt > 0 ? (nowMs - lastUpdateAt) : Infinity;

    const sess = this.#ticketSessionManager._getSession?.(ticketId);

    if (!sess) {
      // No live session — spawn a fresh one with the full context and
      // progress-comment instructions.
      await this.#spawnFreshSession(row);
      return;
    }

    // Live session — decide between escalation, silence-warning, or progress-forced prompt.

    // Escalation: we warned in a prior tick (_warnedSilenceAt is set), we're
    // still past threshold, and the warning has aged at least one poll
    // interval. Kill the session; next tick's "no session" branch respawns.
    if (
      sess._warnedSilenceAt
      && silenceMs > SESSION_SILENCE_WARN_MS
      && (nowMs - sess._warnedSilenceAt) >= TRIGGER_POLL_INTERVAL_MS * 0.9
    ) {
      log(`Ticket poll: escalating — killing silent session ticket=${ticketId} silenceMs=${silenceMs}`);
      try { sess.child.stdin.end(); } catch { /* already closed */ }
      // next tick will respawn; don't send any turn here.
      return;
    }

    // First-level silence warning.
    if (silenceMs > SESSION_SILENCE_WARN_MS && !sess._warnedSilenceAt) {
      log(`Ticket poll: silence warning ticket=${ticketId} silenceMs=${silenceMs}`);
      this.#ticketSessionManager._sendFollowUp(sess, this.#composeSilencePrompt(silenceMs), { checkMaxTurns: false });
      sess._warnedSilenceAt = nowMs;
      return;
    }

    // If activity has resumed (silence below threshold) clear the warn flag so
    // the next silence round can warn again, not escalate straight to kill.
    if (silenceMs <= SESSION_SILENCE_WARN_MS && sess._warnedSilenceAt) {
      sess._warnedSilenceAt = null;
    }

    // Forced progress prompt: session hasn't been nudged in 20 min. Uses
    // lastTouchedAt from the session record (maintained by _sendFollowUp).
    const lastTurnAt = sess.lastTouchedAt || sess.startedAt || nowMs;
    const sinceProgressPromptMs = sess._lastProgressPromptAt
      ? (nowMs - sess._lastProgressPromptAt)
      : Infinity;
    if (
      (nowMs - lastTurnAt) > PROGRESS_PROMPT_INTERVAL_MS
      && sinceProgressPromptMs > PROGRESS_PROMPT_INTERVAL_MS
    ) {
      log(`Ticket poll: 20-min progress prompt ticket=${ticketId}`);
      this.#ticketSessionManager._sendFollowUp(sess, this.#composeProgressPrompt(), { checkMaxTurns: false });
      sess._lastProgressPromptAt = nowMs;
    }
  }

  async #spawnFreshSession(row) {
    const ticketId = row.ticket_id;
    const role = row.role;

    // Full ticket context for the initial prompt. The server didn't send the
    // role_prompt / ticket_prompt / column_prompt in the allocation list (to
    // keep get_allocated_tickets cheap), so we compose via the existing
    // trigger-context path: fetchTicketContext + the session manager's
    // spawn-side prompt composer. dispatchTrigger handles the spawn-or-follow
    // decision; with no live session it'll spawn.
    const ticket = await fetchTicketContext(this.#config, ticketId);
    const rolePrompt = row.role_prompt || ''; // falls back to empty; role knowledge is in agent.role_prompt on the child too
    const ticketPrompt = row.ticket_prompt || '';
    const columnPrompt = row.column_prompt || null;

    // Synthetic trigger_id — no server row. Dedup is a no-op against the
    // live-session map, which is the only dedup that matters now.
    const triggerId = `poll:${ticketId}:${Date.now()}`;

    const dispatch = await this.#ticketSessionManager.dispatchTrigger({
      ticketId,
      triggerId,
      agentId: this.#agentId,
      rolePrompt,
      ticketPrompt,
      columnPrompt,
      ticket,
      // v0.25.0: ask the subagent to leave a progress comment every 20 minutes
      // so the polling supervisor can see forward motion.
      extraInstructions: 'IMPORTANT: While working on this ticket, post a short progress comment (via mcp__awb__add_comment) at least every 20 minutes, even if the update is just "still thinking about X" or "running tests on Y". This gives the supervisor a heartbeat — silence beyond 30 minutes is treated as an error and you may be interrupted with a status-check prompt.',
    });

    if (dispatch?.dispatched) {
      log(`Ticket poll spawned: ticket=${ticketId} role=${role} pid=${dispatch.pid}${dispatch.firstTurn ? ' (new)' : ''}`);
    } else if (dispatch?.reason && dispatch.reason !== 'duplicate_trigger') {
      log(`Ticket poll spawn declined: ticket=${ticketId} reason=${dispatch.reason}`);
    }
  }

  #composeSilencePrompt(silenceMs) {
    const minutes = Math.round(silenceMs / 60_000);
    return [
      `[Supervisor] You have not posted any update to this ticket in ~${minutes} minutes.`,
      '',
      'Post a progress comment NOW explaining:',
      '  - what you are currently doing (or what you are blocked on)',
      '  - what you have completed so far',
      '  - an estimate of time-to-done or what help you need',
      '',
      'Use mcp__awb__add_comment. If you are actually done, move the ticket to the next column via mcp__awb__move_ticket. Silence beyond another 5 minutes will cause your session to be killed and restarted.',
    ].join('\n');
  }

  #composeProgressPrompt() {
    return [
      '[Supervisor] 20-minute progress checkpoint.',
      '',
      'Post a short comment (via mcp__awb__add_comment) summarising what you have done since the last update and what you are working on now. One or two sentences is fine — the goal is a visible heartbeat, not a report.',
    ].join('\n');
  }
}
