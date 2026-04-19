// ─── Ticket Poller ────────────────────────────────────────
// Two paths per tick, executed in priority order:
//
//   1. Busy path (pending-trigger retry). get_pending_triggers returns
//      AgentTriggers the server already created but hasn't yet seen an ack
//      for. Covers SSE drops, silent-exit subagents, and plugin restarts.
//      Whenever this returns anything, we dispatch and stop — the agent
//      already has real work queued up.
//
//   2. Idle path (routing reconciliation). When busy path is empty we ask
//      the server "given the current board routing_config, which tickets
//      would map a role this agent holds AND don't already have a pending
//      trigger?" and fire a manual_trigger on each. This catches tickets
//      that entered a column before the agent existed, columns whose
//      routing was added after the fact, and any state shift that makes a
//      previously-acked ticket actionable again.
//
// Zero column-name / status / role-list hardcoding lives here. Every
// decision — which roles matter for which column, which tickets are
// actionable — is resolved server-side off Board.routing_config. The plugin
// just calls the tools and wakes the ticket-session manager.

import { TRIGGER_POLL_INTERVAL_MS } from './constants.mjs';
import { log } from './logging.mjs';
import { callMcpTool, unwrapToolResult, fireAndForgetTool } from './mcp-client.mjs';
import { fetchTicketContext } from './rest.mjs';
import { loadAgentInfo } from './config.mjs';

// Cooldown between manual_triggers for the same (ticket_id, role). Without
// this an agent whose column_prompt returns "no action needed right now"
// (e.g. promotion conditions not yet met) would be re-spawned every tick
// wasting subagent turns. 5 minutes balances responsiveness to external
// state shifts against spawn-storm avoidance.
const ACTIONABLE_COOLDOWN_MS = 5 * 60_000;

export class TicketPoller {
  #config;
  #agentId;
  #workspaceId;
  #ticketSessionManager;
  #timer = null;
  #stopped = false;
  #busy = false;

  #actionableCooldown = new Map();

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
    // No immediate initial tick. ralf-style harnesses spawn short-lived
    // claude.exe processes (just tools/list then EOF) at 5-second cadence;
    // an immediate tick on every proxy start re-dispatches every stuck
    // pending trigger dozens of times a minute, each spawning a subagent
    // that burns tokens. Wait one full interval — proxies that die before
    // then contribute zero dispatches. A deliberate plugin restart pays a
    // 30s backlog delay, which is acceptable.
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
    // Single-flight: if a previous tick is still working (slow MCP, slow
    // spawn), skip this one rather than stacking duplicate dispatches.
    if (this.#busy) return;
    this.#busy = true;

    try {
      const dispatched = await this.#runBusyPath();
      if (!dispatched && !this.#stopped) {
        await this.#runIdlePath();
      }
    } finally {
      this.#busy = false;
    }
  }

  /**
   * Busy path — dispatch anything the server already has queued up for us.
   * Returns true if at least one pending trigger was processed (idle path
   * will be skipped for this tick).
   */
  async #runBusyPath() {
    const rpc = await callMcpTool(this.#config, 'get_pending_triggers', {
      agent_id: this.#agentId,
      workspace_id: this.#workspaceId,
    }, { clientName: 'awb-ticket-poller' });
    const result = unwrapToolResult(rpc);
    if (result?.error) {
      log(`Ticket poll: get_pending_triggers returned error: ${result.error}`);
      return false;
    }
    const triggers = Array.isArray(result) ? result : (result?.data || []);
    if (!Array.isArray(triggers) || triggers.length === 0) return false;

    log(`Ticket poll (busy): ${triggers.length} pending trigger(s)`);

    for (const trigger of triggers) {
      if (this.#stopped) return true;
      const ticketId = trigger.ticket_id;
      const triggerId = trigger.id;
      if (!ticketId || !triggerId) continue;

      try {
        const ticket = this.#ticketSessionManager.hasSession?.(ticketId)
          ? null
          : await fetchTicketContext(this.#config, ticketId);
        const dispatch = await this.#ticketSessionManager.dispatchTrigger({
          ticketId,
          triggerId,
          agentId: this.#agentId,
          rolePrompt: trigger.role_prompt || '',
          ticketPrompt: trigger.ticket_prompt || '',
          columnPrompt: trigger.column_prompt || null,
          ticket,
        });
        if (dispatch?.dispatched) {
          log(`Ticket poll dispatched: ticket=${ticketId} trigger=${triggerId}${dispatch.firstTurn ? ' (new session)' : ' (follow-up)'}`);
        } else if (dispatch?.reason && dispatch.reason !== 'duplicate_trigger') {
          log(`Ticket poll dispatch declined: ticket=${ticketId} reason=${dispatch.reason}`);
        }
      } catch (err) {
        log(`Ticket poll dispatch failed: ticket=${ticketId} err=${err.message}`);
      }
    }
    return true;
  }

  /**
   * Idle path — ask the server for (ticket, role) pairs that should wake
   * this agent given the current board routing_config, minus anything that
   * already has a pending AgentTrigger. For each eligible pair we fire
   * manual_trigger; the server records the AgentTrigger row and the push /
   * next-tick busy-path then dispatches normally.
   *
   * Column-name / status / role-whitelist logic lives on the server, keyed
   * entirely off Board.routing_config. This method doesn't know the names
   * of any columns.
   */
  async #runIdlePath() {
    const rpc = await callMcpTool(this.#config, 'get_my_actionable_tickets', {
      agent_id: this.#agentId,
      workspace_id: this.#workspaceId,
    }, { clientName: 'awb-ticket-poller-idle' });
    const result = unwrapToolResult(rpc);
    if (result?.error) {
      log(`Ticket poll (idle): get_my_actionable_tickets error: ${result.error}`);
      return;
    }
    const pairs = Array.isArray(result) ? result : (result?.data || []);
    if (!Array.isArray(pairs) || pairs.length === 0) {
      this.#gcCooldowns();
      return;
    }

    const now = Date.now();
    let fired = 0;
    for (const pair of pairs) {
      if (this.#stopped) return;
      const ticketId = pair.ticket_id;
      const role = pair.role;
      if (!ticketId || !role) continue;
      const key = `${ticketId}::${role}`;
      const expiry = this.#actionableCooldown.get(key);
      if (expiry && expiry > now) continue;

      // Fire-and-forget: manual_trigger creates the AgentTrigger server-side
      // which then flows back to us via SSE + (fallback) the next busy-path
      // tick. We don't need the return value here — the row is the signal.
      fireAndForgetTool(this.#config, 'manual_trigger', {
        ticket_id: ticketId,
        role,
      });
      this.#actionableCooldown.set(key, now + ACTIONABLE_COOLDOWN_MS);
      fired++;
    }
    if (fired > 0) log(`Ticket poll (idle): ${fired} actionable ticket(s) manually triggered`);
    this.#gcCooldowns();
  }

  #gcCooldowns() {
    const now = Date.now();
    for (const [k, v] of this.#actionableCooldown) {
      if (v <= now) this.#actionableCooldown.delete(k);
    }
  }
}
