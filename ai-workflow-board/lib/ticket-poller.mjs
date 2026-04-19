// ─── Ticket Poller ────────────────────────────────────────
// Periodic pull-side reconciliation for AgentTrigger delivery. SSE push is
// best-effort: connection drops, server-side filter regressions, and the
// "subagent silent-exits without acknowledging" pattern all lead to triggers
// that exist in the DB with acknowledged_at IS NULL but never reach a
// running subagent. The poller closes that gap by calling
// get_pending_triggers on a fixed interval and dispatching anything the
// live ticket-session map doesn't already cover.
//
// Importantly, this file holds NO routing/role/column logic. Server's
// get_pending_triggers already knows which triggers belong to this agent
// (it filters by agent_id + acknowledged_at IS NULL + expires_at > now).
// We just spawn for whatever the server hands back.

import { TRIGGER_POLL_INTERVAL_MS } from './constants.mjs';
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
      // workspace_id is needed to scope get_pending_triggers. setup may not
      // have persisted it; resolve once via whoami and cache in-memory.
      this.#workspaceId = await this.#resolveWorkspaceId();
      if (!this.#workspaceId) {
        log('Ticket poller skipped — workspace_id resolve via whoami failed');
        return;
      }
    }
    this.#stopped = false;
    // Fire once immediately so a freshly-restarted plugin picks up any
    // backlog without waiting one full interval.
    this.#tick().catch((err) => log(`Ticket poll (initial) failed: ${err.message}`));
    this.#timer = setInterval(() => {
      this.#tick().catch((err) => log(`Ticket poll failed: ${err.message}`));
    }, TRIGGER_POLL_INTERVAL_MS);
    this.#timer.unref?.();
    log(`Ticket poller started (agent=${this.#agentId.slice(0, 8)} workspace=${this.#workspaceId.slice(0, 8)} interval=${TRIGGER_POLL_INTERVAL_MS / 1000}s)`);
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
    // Single-flight: if a previous tick is still working (slow MCP, slow
    // spawn), skip this one rather than stacking duplicate dispatches.
    if (this.#busy) return;
    this.#busy = true;

    try {
      const rpc = await callMcpTool(this.#config, 'get_pending_triggers', {
        agent_id: this.#agentId,
        workspace_id: this.#workspaceId,
      }, { clientName: 'awb-ticket-poller' });
      const result = unwrapToolResult(rpc);
      if (result?.error) {
        log(`Ticket poll: get_pending_triggers returned error: ${result.error}`);
        return;
      }
      const triggers = Array.isArray(result) ? result : (result?.data || []);
      if (!Array.isArray(triggers) || triggers.length === 0) return;

      log(`Ticket poll: ${triggers.length} pending trigger(s)`);

      for (const trigger of triggers) {
        if (this.#stopped) return;
        const ticketId = trigger.ticket_id;
        const triggerId = trigger.id;
        if (!ticketId || !triggerId) continue;

        // We do NOT pre-filter on hasSession. dispatchTrigger handles both
        // paths cleanly: alive session → follow-up turn (so a sleeping
        // session is woken with the new trigger), no session → fresh spawn.
        // Dedup inside the manager protects against the SSE-then-poll race
        // for an identical trigger; that dedup is released on session exit
        // so a silent / errored subagent's triggers become re-dispatchable.
        try {
          // Skip the ticket fetch when a session is already alive — it's
          // about to receive a follow-up turn and can re-fetch via MCP if
          // it actually needs fresh state. Saves a round-trip on the hot
          // path (active sessions getting follow-ups every poll tick).
          const ticket = this.#ticketSessionManager.hasSession?.(ticketId)
            ? null
            : await fetchTicketContext(this.#config, ticketId);
          const result = await this.#ticketSessionManager.dispatchTrigger({
            ticketId,
            triggerId,
            agentId: this.#agentId,
            rolePrompt: trigger.role_prompt || '',
            ticketPrompt: trigger.ticket_prompt || '',
            columnPrompt: trigger.column_prompt || null,
            ticket,
          });
          if (result?.dispatched) {
            log(`Ticket poll dispatched: ticket=${ticketId} trigger=${triggerId}${result.firstTurn ? ' (new session)' : ' (follow-up)'}`);
          } else if (result?.reason && result.reason !== 'duplicate_trigger') {
            log(`Ticket poll dispatch declined: ticket=${ticketId} reason=${result.reason}`);
          }
        } catch (err) {
          log(`Ticket poll dispatch failed: ticket=${ticketId} err=${err.message}`);
        }
      }
    } finally {
      this.#busy = false;
    }
  }
}
