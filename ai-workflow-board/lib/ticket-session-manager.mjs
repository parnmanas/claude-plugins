// ─── Ticket Session Manager (v0.8.0 persistent per-ticket subagents) ────

import { BaseSessionManager } from './base-session-manager.mjs';
import { composeTriggerPrompt } from './prompts.mjs';
import { fireAndForgetTool } from './mcp-client.mjs';
import { log } from './logging.mjs';

/**
 * Keeps one Claude CLI child alive per ticket so that successive events
 * (trigger, board_update, comments) reuse the same KV cache and context.
 *
 * Mirrors ChatSessionManager's lifecycle model:
 *   - IDLE_TTL:   no events → stdin.end() → child exits
 *   - MAX_TURNS:  respawn on next event (fresh context)
 *   - CAP:        LRU-evict oldest-idle before spawn
 *
 * Trigger events spawn a new session with full ticket context.
 * Board_update events are forwarded to existing sessions as follow-up turns.
 */
export class TicketSessionManager extends BaseSessionManager {
  constructor(config) {
    super(config, {
      keyField: 'ticketId',
      logTag: '[ticket-session]',
      cfgPrefix: 'cfg-ticket-',
      kindLabel: 'ticket_session',
    });
  }

  /**
   * Dispatch a trigger into the ticket's live session, spawning one if needed.
   * spec = { ticketId, triggerId, agentId, rolePrompt, ticketPrompt, columnPrompt, ticket }
   * Returns { dispatched: boolean, pid?: number, reason?: string, firstTurn?: boolean }
   */
  async dispatchTrigger(spec) {
    if (!spec.ticketId) return { dispatched: false, reason: 'no_ticket' };

    // Dedup by triggerId — scoped to a specific session's lifetime via
    // _forgetDedup on exit (see #wireSessionLifecycle). Same key returning
    // 'duplicate_trigger' here means the live session is already going to
    // process this trigger; the poller's retry path becomes active again
    // only after the session exits.
    const dedupKey = spec.triggerId ? `trigger:${spec.triggerId}` : null;
    if (dedupKey && !this._rememberDedup(dedupKey)) {
      return { dispatched: false, reason: 'duplicate_trigger' };
    }

    const sess = this._getSession(spec.ticketId);

    if (sess) {
      // Existing live session — send the trigger as a follow-up turn.
      // Replace the pending-ack target with this trigger so the next
      // successful result acks the right one. set_current_task already
      // fired from the original spawn; no need to repeat.
      this._sendFollowUp(sess, this.#composeTriggerTurn(spec));
      this.#trackTrigger(sess, spec.triggerId, spec.agentId);
      return { dispatched: true, pid: sess.pid };
    }

    // No live session — spawn. Check cap; LRU-evict on overflow.
    if (!this._ensureCapacity()) {
      // Roll back the dedup so the next poll tick can retry once capacity frees up.
      if (dedupKey) this._forgetDedup(dedupKey);
      return { dispatched: false, reason: 'cap_busy' };
    }

    const firstTurnText = composeTriggerPrompt(
      spec.ticket, spec.rolePrompt || '', spec.ticketPrompt || '', spec.ticketId, spec.columnPrompt || null,
    );
    const spawned = await this._spawnSession(spec.ticketId, spec.rolePrompt || '', firstTurnText);
    if (!spawned) {
      if (dedupKey) this._forgetDedup(dedupKey);
      return { dispatched: false, reason: 'spawn_failed' };
    }

    // Stamp identity onto the session and arm lifecycle hooks.
    spawned.agentId = spec.agentId || '';
    spawned.triggerKeys = [];      // dedup keys we hold; cleared on exit
    spawned.pendingAckTriggerId = null; // last trigger waiting to be acked on result

    this.#trackTrigger(spawned, spec.triggerId, spec.agentId);

    if (spawned.agentId) {
      // Dashboard "processing" badge — only flips on now that a real child
      // is alive. Cleared by the exit hook below.
      fireAndForgetTool(this._config, 'set_current_task', {
        agent_id: spawned.agentId,
        ticket_id: spec.ticketId,
      });
    } else {
      log('[ticket-session] dispatched without agentId — skipping current_task / ack signals');
    }

    // Result hook: ack the latest trigger ONLY when a turn actually
    // completed without error. Silent / errored exits leave the trigger
    // unacknowledged so the poller retries.
    spawned.onResult = (parsed) => {
      const tid = spawned.pendingAckTriggerId;
      if (!tid || !spawned.agentId) return;
      if (parsed?.is_error) return;
      fireAndForgetTool(this._config, 'acknowledge_trigger', {
        trigger_id: tid,
        agent_id: spawned.agentId,
      });
      // Clear so the next turn's result doesn't re-ack the same trigger.
      // The next dispatch (follow-up or new trigger) sets a fresh one.
      spawned.pendingAckTriggerId = null;
    };

    // Exit hook — runs after base-session-manager's #wireExit. Releases
    // every dedup key the session held (so the poller can re-dispatch any
    // unacknowledged trigger) and clears the dashboard badge.
    spawned.child.once('exit', () => {
      for (const k of spawned.triggerKeys) this._forgetDedup(k);
      spawned.triggerKeys = [];
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
   * Record the trigger on the session so the result hook can ack it and
   * the exit hook can release its dedup entry.
   */
  #trackTrigger(sess, triggerId, agentId) {
    if (!triggerId) return;
    sess.triggerKeys ??= [];
    sess.triggerKeys.push(`trigger:${triggerId}`);
    sess.pendingAckTriggerId = triggerId;
    if (agentId && !sess.agentId) sess.agentId = agentId;
  }

  /**
   * Forward a comment_mention event to an existing ticket session.
   * Unlike forwardBoardUpdate, this leads with a strong "addressed to YOU"
   * banner so the session's next turn treats the comment as a direct request
   * rather than ambient activity.
   * Returns true if delivered, false if no live session exists for this ticket.
   */
  forwardCommentMention(ticketId, mention) {
    const sess = this._getSession(ticketId);
    if (!sess) return false;

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

    this._sendFollowUp(sess, lines.join('\n'), { checkMaxTurns: false });
    return true;
  }

  /**
   * Forward a board_update event to an existing ticket session.
   * Returns true if delivered, false if no live session exists for this ticket.
   */
  forwardBoardUpdate(ticketId, ev) {
    const sess = this._getSession(ticketId);
    if (!sess) return false;

    const lines = [];
    lines.push(`[Board Update] The ticket you are working on was updated:`);
    lines.push(`  Event: ${ev.entity_type || 'unknown'}.${ev.action || 'unknown'}`);
    if (ev.field_changed) lines.push(`  Field changed: ${ev.field_changed}`);
    if (ev.actor_name) lines.push(`  By: ${ev.actor_name}`);
    lines.push('');
    lines.push('Review the change and adjust your work if needed. Use mcp__awb__get_ticket to fetch the latest ticket state.');

    this._sendFollowUp(sess, lines.join('\n'), { checkMaxTurns: false });
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
      ticketId: s.ticketId,
      pid: s.pid,
      turnCount: s.turnCount,
      startedAt: s.startedAt,
      lastTouchedAt: s.lastTouchedAt,
    }));
  }
}
