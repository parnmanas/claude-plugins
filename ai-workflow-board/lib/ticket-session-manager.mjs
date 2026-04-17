// ─── Ticket Session Manager (v0.8.0 persistent per-ticket subagents) ────

import { BaseSessionManager } from './base-session-manager.mjs';
import { composeTriggerPrompt } from './prompts.mjs';

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

    // Dedup by triggerId
    if (spec.triggerId) {
      const dedupKey = `trigger:${spec.triggerId}`;
      if (!this._rememberDedup(dedupKey)) {
        return { dispatched: false, reason: 'duplicate_trigger' };
      }
    }

    const sess = this._getSession(spec.ticketId);

    if (sess) {
      // Existing live session — send the trigger as a follow-up turn
      this._sendFollowUp(sess, this.#composeTriggerTurn(spec));
      return { dispatched: true, pid: sess.pid };
    }

    // No live session — spawn. Check cap; LRU-evict on overflow.
    if (!this._ensureCapacity()) {
      return { dispatched: false, reason: 'cap_busy' };
    }

    const firstTurnText = composeTriggerPrompt(
      spec.ticket, spec.rolePrompt || '', spec.ticketPrompt || '', spec.ticketId, spec.columnPrompt || null,
    );
    const spawned = await this._spawnSession(spec.ticketId, spec.rolePrompt || '', firstTurnText);
    if (!spawned) return { dispatched: false, reason: 'spawn_failed' };
    return { dispatched: true, pid: spawned.pid, firstTurn: true };
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
