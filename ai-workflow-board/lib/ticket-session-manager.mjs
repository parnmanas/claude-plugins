// ─── Ticket Session Manager (v0.8.0 persistent per-ticket subagents) ────

import { BaseSessionManager } from './base-session-manager.mjs';
import { composeTriggerPrompt } from './prompts.mjs';
import { fireAndForgetTool } from './mcp-client.mjs';
import { log } from './logging.mjs';

/**
 * Keeps one Claude CLI child alive per (ticket, role) so successive events
 * for the same role reuse the same KV cache and context.
 *
 * Per-role, not per-ticket: the same agent can hold multiple roles on one
 * ticket (assignee + reviewer, etc.), and each role has its own role_prompt
 * and a different scope of responsibility. Sharing one subagent across
 * roles would mix the role prompts (only one gets appended to the system
 * prompt at spawn time), let work done as "assignee" bleed into
 * reviewer-style responses, and generally corrupt per-role bookkeeping on
 * the server (claim_ticket, my_last_update_at, etc.). Session key is the
 * composite `${ticketId}:${role}` so each role gets its own child.
 *
 * Forwarded events (board_update, comment mention) that aren't role-scoped
 * fan out to every live session for the ticket — every role that's working
 * on it needs to see the change.
 *
 * Lifecycle mirrors ChatSessionManager:
 *   - IDLE_TTL:   no events → stdin.end() → child exits
 *   - MAX_TURNS:  respawn on next event (fresh context)
 *   - CAP:        LRU-evict oldest-idle before spawn
 */
export class TicketSessionManager extends BaseSessionManager {
  constructor(config) {
    super(config, {
      // Key is now a composite `${ticketId}:${role}`. The base class treats
      // the keyField value as opaque, so we just rename the field and store
      // ticketId + role separately on the session record for the fan-out
      // helpers below.
      keyField: 'sessionKey',
      logTag: '[ticket-session]',
      cfgPrefix: 'cfg-ticket-',
      kindLabel: 'ticket_session',
    });
  }

  #makeKey(ticketId, role) {
    // Role defaults to '_' for triggers that arrive without a role
    // (shouldn't happen in practice, but keeps the key well-formed). The
    // separator is ':' — role strings are always one of
    // assignee/reporter/reviewer/'_', none of which contain a colon.
    return `${ticketId}:${role || '_'}`;
  }

  /**
   * Dispatch a trigger into the (ticket, role) live session, spawning one if needed.
   * spec = { ticketId, role, triggerId, agentId, rolePrompt, ticketPrompt, columnPrompt, ticket, extraInstructions?, forceRespawn? }
   * Returns { dispatched: boolean, pid?: number, reason?: string, firstTurn?: boolean }
   *
   * v0.26.0: server-side TicketSupervisorService replaces the plugin's former
   * 5-minute get_allocated_tickets poll. Dedup is still useful to prevent
   * SSE double-dispatch of the same trigger_id into a live session.
   * `spec.forceRespawn === true` is the escalation signal: kill the session
   * for this (ticket, role) so the spawn branch produces a fresh child.
   * Sessions for OTHER roles on the same ticket are untouched — a respawn
   * request for assignee shouldn't kill the reviewer's in-flight work.
   */
  async dispatchTrigger(spec) {
    if (!spec.ticketId) return { dispatched: false, reason: 'no_ticket' };
    const role = spec.role || '';
    const sessionKey = this.#makeKey(spec.ticketId, role);

    const dedupKey = spec.triggerId ? `trigger:${spec.triggerId}` : null;
    if (dedupKey && !this._rememberDedup(dedupKey)) {
      return { dispatched: false, reason: 'duplicate_trigger' };
    }

    if (spec.forceRespawn === true) {
      const prev = this._getSession(sessionKey);
      if (prev) {
        log(`Ticket session force-respawn requested: ticket=${spec.ticketId} role=${role} pid=${prev.pid}`);
        if (prev.idleTimer) { clearTimeout(prev.idleTimer); prev.idleTimer = null; }
        try { prev.child.stdin.end(); } catch { /* already closed */ }
        try { process.kill(prev.pid, 'SIGTERM'); } catch { /* already dead */ }
        this._sessions.delete(sessionKey);
      }
    }

    const sess = this._getSession(sessionKey);

    if (sess) {
      // Existing live session — send as follow-up turn. set_current_task
      // already fired from original spawn; no need to repeat.
      this._sendFollowUp(sess, this.#composeTriggerTurn(spec));
      if (spec.agentId && !sess.agentId) sess.agentId = spec.agentId;
      return { dispatched: true, pid: sess.pid };
    }

    // No live session for this (ticket, role) — spawn. Check cap;
    // LRU-evict on overflow.
    if (!this._ensureCapacity()) {
      if (dedupKey) this._forgetDedup(dedupKey);
      return { dispatched: false, reason: 'cap_busy' };
    }

    const firstTurnText = composeTriggerPrompt(
      spec.ticket, spec.rolePrompt || '', spec.ticketPrompt || '',
      spec.ticketId, spec.columnPrompt || null,
      spec.extraInstructions || null,
    );
    const spawned = await this._spawnSession(sessionKey, spec.rolePrompt || '', firstTurnText);
    if (!spawned) {
      if (dedupKey) this._forgetDedup(dedupKey);
      return { dispatched: false, reason: 'spawn_failed' };
    }

    // Denormalise ticketId + role onto the session record for the fan-out
    // helpers below. keyField on the base-class record is now `sessionKey`.
    spawned.ticketId = spec.ticketId;
    spawned.role = role;
    spawned.agentId = spec.agentId || '';

    if (spawned.agentId) {
      // Dashboard "processing" badge — cleared by the exit hook below.
      fireAndForgetTool(this._config, 'set_current_task', {
        agent_id: spawned.agentId,
        ticket_id: spec.ticketId,
      });
    }

    // Exit hook — clear the dashboard badge + release the dedup key so a
    // future SSE/poll dispatch for the same trigger_id (unlikely, but possible
    // with SSE reconnect replays) can spawn a fresh session.
    spawned.child.once('exit', () => {
      if (dedupKey) this._forgetDedup(dedupKey);
      if (spawned.agentId) {
        fireAndForgetTool(this._config, 'clear_current_task', {
          agent_id: spawned.agentId,
          ticket_id: spec.ticketId,
        });
      }
    });

    return { dispatched: true, pid: spawned.pid, firstTurn: true };
  }

  /**
   * Return every live session for the given ticketId, across all roles.
   * Used by the fan-out forward helpers below so a board_update or
   * comment_mention reaches every subagent working on the ticket.
   */
  #sessionsForTicket(ticketId) {
    const hits = [];
    for (const sess of this._sessions.values()) {
      if (sess.ticketId === ticketId) hits.push(sess);
    }
    return hits;
  }

  /**
   * Forward a comment_mention event to every role session for this ticket.
   * Comments don't carry a role (they're ticket-scoped), so fan out to all
   * live sessions — each role may have its own reaction based on its
   * responsibility.
   *
   * Leads with an "addressed to YOU" banner so the session's next turn
   * treats the comment as a direct request rather than ambient activity.
   * If the mention used a role shortcut (@assignee / @reviewer / ...) and
   * we have a session for that exact role, we deliver ONLY to that role —
   * fan-out would be noise there.
   *
   * Returns true if at least one session received the mention.
   */
  forwardCommentMention(ticketId, mention) {
    const sessions = this.#sessionsForTicket(ticketId);
    if (sessions.length === 0) return false;

    const lines = [];
    lines.push('⚠️ [Comment Mention] You were @-mentioned in a comment on this ticket. This is addressed to YOU — respond directly.');
    if (mention.actor_name) lines.push(`  By: ${mention.actor_name}`);
    if (mention.mention_source === 'role' && mention.role_shortcut) {
      lines.push(`  Via role shortcut: @${mention.role_shortcut}`);
    }
    lines.push('');
    lines.push('Comment body:');
    lines.push(mention.content || '');
    lines.push('');
    lines.push('Read the comment and respond to the request directly. Use mcp__awb__get_ticket if you need fresh ticket state, and leave a reply comment addressing the user.');
    const text = lines.join('\n');

    // If the mention targeted a specific role via @assignee / @reviewer and
    // we have a session for that role, deliver only there. Otherwise fan
    // out to every role session so whoever is best-placed can respond.
    const targetedRole = mention.mention_source === 'role' ? mention.role_shortcut : null;
    const targets = targetedRole
      ? sessions.filter((s) => s.role === targetedRole)
      : sessions;
    const recipients = targets.length > 0 ? targets : sessions;

    for (const sess of recipients) {
      this._sendFollowUp(sess, text, { checkMaxTurns: false });
    }
    return true;
  }

  /**
   * Forward a board_update event to every role session for this ticket —
   * every role working on the ticket needs to see a field change / move.
   * Returns true if at least one session received the update.
   */
  forwardBoardUpdate(ticketId, ev) {
    const sessions = this.#sessionsForTicket(ticketId);
    if (sessions.length === 0) return false;

    const lines = [];
    lines.push(`[Board Update] The ticket you are working on was updated:`);
    lines.push(`  Event: ${ev.entity_type || 'unknown'}.${ev.action || 'unknown'}`);
    if (ev.field_changed) lines.push(`  Field changed: ${ev.field_changed}`);
    if (ev.actor_name) lines.push(`  By: ${ev.actor_name}`);
    lines.push('');
    lines.push('Review the change and adjust your work if needed. Use mcp__awb__get_ticket to fetch the latest ticket state.');
    const text = lines.join('\n');

    for (const sess of sessions) {
      this._sendFollowUp(sess, text, { checkMaxTurns: false });
    }
    return true;
  }

  #composeTriggerTurn(spec) {
    const lines = [];
    lines.push(`[New Trigger] A new trigger arrived for the ticket you are already working on.`);
    if (spec.columnPrompt && spec.columnPrompt.content) {
      lines.push('');
      lines.push(`Column workflow guide (${spec.columnPrompt.name || 'column_prompt'}):`);
      lines.push(spec.columnPrompt.content);
    }
    if (spec.ticketPrompt) {
      lines.push('');
      lines.push('Updated instructions:');
      lines.push(spec.ticketPrompt);
    }
    lines.push('');
    lines.push('Use mcp__awb__get_ticket to fetch the latest ticket state and continue your work.');
    return lines.join('\n');
  }

  /** Test-only accessor: snapshot of active sessions. */
  _snapshot() {
    return Array.from(this._sessions.values()).map((s) => ({
      sessionKey: s.sessionKey,
      ticketId: s.ticketId,
      role: s.role,
      pid: s.pid,
      turnCount: s.turnCount,
      startedAt: s.startedAt,
      lastTouchedAt: s.lastTouchedAt,
    }));
  }
}
