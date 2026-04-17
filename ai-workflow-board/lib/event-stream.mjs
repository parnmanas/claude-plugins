// ─── SSE Event Stream ─────────────────────────────────────

import { RECONNECT_INITIAL_MS, RECONNECT_MAX_MS } from './constants.mjs';
import { log, sendChannelEvent } from './logging.mjs';
import { loadAgentInfo } from './config.mjs';
import { fetchTicketContext, fetchChatRoomHistory } from './rest.mjs';
import {
  composeTriggerPrompt,
  composeChatPrompt,
  composeChatRoomPrompt,
} from './prompts.mjs';

/**
 * Connect to AWB's SSE /api/events/stream and forward ticket events
 * as claude/channel notifications. Reconnects with exponential backoff.
 *
 * AWB SSE event types (from events.controller.ts):
 *   - board_update:  ticket/comment CRUD (entity_type, action, field_changed, actor_name)
 *   - agent_trigger: trigger assigned to agent (role, trigger_id, agent_id)
 *   - agent_typing:  typing indicator (ignored by proxy)
 */
export class EventStream {
  #url;
  #retryDelay = RECONNECT_INITIAL_MS;
  #abortController = null;
  #stopped = false;
  #config;                    // Phase 4 Plan 04-03 — delegation branch decisions
  #subagentManager;           // Phase 4 Plan 04-03 — spawn target (may be null)
  #chatSessionManager;        // v0.7.0 — persistent per-room chat sessions (may be null)
  #ticketSessionManager;      // v0.8.0 — persistent per-ticket sessions (may be null)

  constructor(config, subagentManager = null, chatSessionManager = null, ticketSessionManager = null) {
    this.#url = `${config.url.replace(/\/$/, '')}/api/events/stream?token=${encodeURIComponent(config.apiKey)}`;
    this.#config = config;
    this.#subagentManager = subagentManager;
    this.#chatSessionManager = chatSessionManager;
    this.#ticketSessionManager = ticketSessionManager;
  }

  start() {
    this.#stopped = false;
    this.#connect();
  }

  stop() {
    this.#stopped = true;
    this.#abortController?.abort();
  }

  async #connect() {
    if (this.#stopped) return;

    try {
      this.#abortController = new AbortController();
      const resp = await fetch(this.#url, {
        headers: { Accept: 'text/event-stream' },
        signal: this.#abortController.signal,
      });

      if (!resp.ok) {
        log(`SSE error: ${resp.status} ${resp.statusText}`);
        this.#scheduleReconnect();
        return;
      }

      log('SSE connected');
      this.#retryDelay = RECONNECT_INITIAL_MS;
      await this.#readStream(resp.body);

      // Stream ended cleanly — reconnect
      log('SSE stream ended, reconnecting...');
      this.#scheduleReconnect();
    } catch (err) {
      if (err.name === 'AbortError') return;
      log(`SSE error: ${err.message}`);
      this.#scheduleReconnect();
    }
  }

  async #readStream(body) {
    const reader = body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    let eventType = '';

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop(); // keep incomplete line

      for (const line of lines) {
        if (line.startsWith('event: ')) {
          // SSE event type field — NestJS sets this from MessageEvent.type
          eventType = line.slice(7).trim();
        } else if (line.startsWith('data: ')) {
          const data = line.slice(6).trim();
          if (data && eventType === 'agent_trigger') {
            this.#handleTrigger(data);
          } else if (data && eventType === 'board_update') {
            this.#handleBoardUpdate(data);
          } else if (data && eventType === 'chat_request') {
            this.#handleChatRequest(data);
          } else if (data && eventType === 'chat_room_message') {
            this.#handleChatRoomMessage(data);
          }
          // Reset after processing data (SSE spec: dispatch on blank line,
          // but we process eagerly since each event: + data: pair is atomic)
          eventType = '';
        } else if (line === '') {
          // Blank line = end of SSE event block
          eventType = '';
        }
      }
    }
  }

  async #handleTrigger(raw) {
    let ev;
    try {
      ev = JSON.parse(raw);
    } catch (err) {
      log(`Failed to parse trigger: ${err.message}`);
      return;
    }

    // Phase 1 flatten-on-emit asymmetry: agent_trigger reads TOP-LEVEL fields
    // (ev.role_prompt, ev.ticket_prompt, ev.ticket_id, ev.field_changed, ev.actor_name).
    // In contrast, chat_request is envelope-native and reads ev.payload.* — see
    // #handleChatRequest for the other side of this asymmetry.

    // D-59: delegation branch — check config flag AND runtime capacity before choosing path
    // Default true when config key absent: undefined !== false evaluates to true.
    const delegationEnabled = this.#config?.delegation?.enabled !== false;
    const persistentTicket = this.#config?.delegation?.persistentTicketSessions !== false;

    // v0.8.0: prefer persistent per-ticket session path. The session stays alive
    // across multiple triggers and board_update events for the same ticket, reusing
    // KV cache and maintaining full context.
    if (delegationEnabled && persistentTicket && this.#ticketSessionManager) {
      try {
        const ticket = await fetchTicketContext(this.#config, ev.ticket_id);
        const rolePrompt = ev.role_prompt || '';
        const ticketPrompt = ev.ticket_prompt || '';

        const result = await this.#ticketSessionManager.dispatchTrigger({
          ticketId: ev.ticket_id || '',
          triggerId: ev.field_changed || '',
          agentId: ev.actor_name || '',
          rolePrompt,
          ticketPrompt,
          ticket,
        });

        if (result.dispatched) {
          log(`Trigger dispatched to ticket session: ticket=${ev.ticket_id} pid=${result.pid}${result.firstTurn ? ' (new session)' : ''}`);
          return;
        }
        if (result.reason === 'duplicate_trigger') {
          log(`Trigger deduped: ticket=${ev.ticket_id} trigger=${ev.field_changed || ''}`);
          return;
        }
        log(`Ticket session dispatch declined (${result.reason}), falling back to one-shot subagent`);
      } catch (err) {
        log(`Ticket session path failed: ${err.message}, falling back to one-shot subagent`);
      }
    }

    // Fallback: one-shot subagent (legacy Phase 4 behavior)
    const canDelegate = delegationEnabled && this.#subagentManager && this.#subagentManager.canSpawn();

    if (canDelegate) {
      try {
        const ticket = await fetchTicketContext(this.#config, ev.ticket_id);
        const rolePrompt = ev.role_prompt || '';
        const ticketPrompt = ev.ticket_prompt || '';
        const taskText = composeTriggerPrompt(ticket, rolePrompt, ticketPrompt, ev.ticket_id);

        const result = await this.#subagentManager.spawn({
          kind: 'trigger',
          taskText,
          rolePrompt,
          triggerId: ev.field_changed || '',
          ticketId: ev.ticket_id || '',
          agentId: ev.actor_name || '',
        });

        if (result.spawned) {
          log(`Trigger dispatched to subagent: ticket=${ev.ticket_id} pid=${result.pid}`);
          return;
        }
        log(`Subagent spawn declined (${result.reason}), falling back to main session forward`);
      } catch (err) {
        log(`Delegation path failed: ${err.message}, falling back to main session forward`);
      }
    }

    // ── Legacy Phase 1 pass-through path ─────────────────────────────
    // Runs when: delegation disabled OR subagentManager missing OR canSpawn false
    // OR spawn declined OR delegation path threw. Preserves exact Phase 1 behavior
    // for users who set delegation.enabled: false (or who have no delegation config).
    // AWB events.controller uses: action=role, field_changed=trigger_id, actor_name=agent_id
    sendChannelEvent(
      `[AWB Trigger] ticket=${ev.ticket_id} role=${ev.action} trigger=${ev.field_changed}`,
      {
        type: 'agent_trigger',
        ticket_id: ev.ticket_id || '',
        trigger_id: ev.field_changed || '',
        agent_id: ev.actor_name || '',
        role: ev.action || '',
        timestamp: ev.timestamp || new Date().toISOString(),
      },
    );
    log(`Trigger forwarded (legacy path): ticket=${ev.ticket_id} role=${ev.action}`);
  }

  async #handleChatRequest(raw) {
    let ev;
    try {
      ev = JSON.parse(raw);
    } catch (err) {
      log(`Failed to parse chat_request: ${err.message}`);
      return;
    }

    // ASYMMETRY NOTICE: chat_request ships envelope-native (Plan 04-01), so fields
    // live under ev.payload.*  —  NOT at the top level like agent_trigger. This is
    // the counterpart to the flatten-on-emit path in #handleTrigger above. See
    // 01-02-SUMMARY.md:203 and 04-01-SUMMARY.md for the rationale.
    const payload = ev.payload || {};
    // Default true when config key absent: undefined !== false evaluates to true.
    const delegationEnabled = this.#config?.delegation?.enabled !== false;
    const persistentChat = this.#config?.delegation?.persistentChatSessions !== false;

    // v0.7.0: prefer the persistent per-room session path when we have a room_id.
    // Falls back to the legacy per-message spawn when the flag is disabled OR the
    // event carries no room_id (shouldn't happen after the server-side fix, but the
    // older SubagentManager path still covers the degenerate case).
    if (delegationEnabled && persistentChat && this.#chatSessionManager && payload.room_id) {
      try {
        const result = await this.#chatSessionManager.dispatch({
          roomId: payload.room_id,
          senderId: payload.user_id || '',
          senderName: '',
          createdAt: ev.timestamp || '',
          content: payload.new_message || '',
          rolePrompt: payload.role_prompt || '',
        });
        if (result.dispatched) {
          log(`Chat request dispatched to session: room=${payload.room_id} pid=${result.pid}${result.firstTurn ? ' (new session)' : ''}`);
          return;
        }
        if (result.reason === 'duplicate_chat') {
          log(`Chat request deduped: room=${payload.room_id} user=${payload.user_id} ts=${ev.timestamp || ''}`);
          return;
        }
        log(`Chat session dispatch declined (${result.reason}), falling back to legacy path`);
      } catch (err) {
        log(`Chat session path failed: ${err.message}, falling back to legacy path`);
      }
    }

    const canDelegate = delegationEnabled && this.#subagentManager && this.#subagentManager.canSpawn();

    if (canDelegate) {
      const rolePrompt = payload.role_prompt || '';
      const history = Array.isArray(payload.history) ? payload.history : [];
      const newMessage = payload.new_message || '';
      const taskText = composeChatPrompt(rolePrompt, history, newMessage);

      try {
        const result = await this.#subagentManager.spawn({
          kind: 'chat',
          taskText,
          rolePrompt,
          // v0.6.11: unified dedup key so chat_request and chat_room_message — both emitted
          // for the same user message (same savedMsg.created_at) — collide on spawn().
          // Previously each handler used a different key format and double-spawned.
          chatRequestId: payload.user_id
            ? `msg:${payload.user_id}:${ev.timestamp || ''}`
            : undefined,
          ticketId: payload.ticket_id || '',
          agentId: payload.agent_id || '',
        });

        if (result.spawned) {
          log(`Chat request dispatched to subagent: agent=${payload.agent_id} pid=${result.pid}`);
          return;
        }
        log(`Chat subagent spawn declined (${result.reason}), falling back to main session forward`);
      } catch (err) {
        log(`Chat delegation path failed: ${err.message}, falling back to main session forward`);
      }
    }

    // ── Fallback: notify main session ───────────────────────────────
    // Runs when: delegation disabled OR subagentManager missing OR canSpawn false
    // OR spawn declined OR delegation path threw.
    sendChannelEvent(
      `[AWB Chat Request] agent=${payload.agent_id} user=${payload.user_id} room=${payload.room_id || ''}`,
      {
        type: 'chat_request',
        agent_id: payload.agent_id || '',
        user_id: payload.user_id || '',
        room_id: payload.room_id || '',
        ticket_id: payload.ticket_id || '',
        new_message: payload.new_message || '',
        timestamp: ev.timestamp || new Date().toISOString(),
      },
    );
    log(`Chat request forwarded (fallback path): agent=${payload.agent_id} user=${payload.user_id}`);
  }

  #handleBoardUpdate(raw) {
    try {
      const ev = JSON.parse(raw);
      // entity_type: 'ticket' | 'comment' | 'child_ticket' etc.
      // action: 'created' | 'updated' | 'moved' | 'deleted' | 'status_changed'

      // v0.8.0: if a persistent ticket session exists for this ticket, forward the
      // update there so the subagent can react in context. Don't send to main session.
      if (this.#ticketSessionManager && ev.ticket_id) {
        const forwarded = this.#ticketSessionManager.forwardBoardUpdate(ev.ticket_id, ev);
        if (forwarded) {
          log(`Board update forwarded to ticket session: ticket=${ev.ticket_id} ${ev.entity_type}.${ev.action}`);
          return;
        }
      }

      // No live ticket session — forward to main session as before
      const label = ev.entity_type === 'comment' ? 'Comment' : 'Update';
      sendChannelEvent(
        `[AWB ${label}] ticket=${ev.ticket_id} ${ev.entity_type}.${ev.action}${ev.field_changed ? ` field=${ev.field_changed}` : ''} by=${ev.actor_name}`,
        {
          type: 'board_update',
          ticket_id: ev.ticket_id || '',
          entity_type: ev.entity_type || '',
          action: ev.action || '',
          field_changed: ev.field_changed || '',
          actor_name: ev.actor_name || '',
          timestamp: ev.timestamp || new Date().toISOString(),
        },
      );
      log(`Board update forwarded: ticket=${ev.ticket_id} ${ev.entity_type}.${ev.action}`);
    } catch (err) {
      log(`Failed to parse board_update: ${err.message}`);
    }
  }

  async #setChatRoomTyping(roomId, isTyping, status = null) {
    try {
      const agentInfo = loadAgentInfo();
      const url = `${this.#config.url.replace(/\/$/, '')}/api/agent/chat-rooms/${encodeURIComponent(roomId)}/typing`;
      await fetch(url, {
        method: 'POST',
        headers: {
          'X-Agent-Key': this.#config.apiKey,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          agent_id: agentInfo?.agent_id || '',
          agent_name: agentInfo?.name || agentInfo?.agent_name || 'Agent',
          is_typing: isTyping,
          status: status,
        }),
        signal: AbortSignal.timeout(5000),
      });
    } catch (err) {
      log(`setChatRoomTyping failed: ${err.message}`);
    }
  }

  async #sendChatRoomAck(roomId, content) {
    try {
      const agentInfo = loadAgentInfo();
      const url = `${this.#config.url.replace(/\/$/, '')}/api/agent/chat-rooms/${encodeURIComponent(roomId)}/messages`;
      await fetch(url, {
        method: 'POST',
        headers: {
          'X-Agent-Key': this.#config.apiKey,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          agent_id: agentInfo?.agent_id || '',
          content,
        }),
        signal: AbortSignal.timeout(5000),
      });
    } catch (err) {
      log(`sendChatRoomAck failed: ${err.message}`);
    }
  }

  async #handleChatRoomMessage(raw) {
    let ev;
    try {
      ev = JSON.parse(raw);
    } catch (err) {
      log(`Failed to parse chat_room_message: ${err.message}`);
      return;
    }

    const p = ev.payload || ev;

    // Skip messages sent by agents to avoid self-reply loops. Still record into
    // the ring so subsequent dispatches see them as conversation history.
    if (p.sender_type === 'agent') {
      this.#chatSessionManager?.recordRoomMessage(p);
      log(`Chat room message from agent (${p.sender_name || p.sender_id}) — skipping delegation`);
      return;
    }

    // Immediate visual feedback via typing indicator — ephemeral, auto-clears
    // when the real reply arrives. Combining the emoji into status keeps it
    // out of the persisted message log so we don't accumulate stale acks.
    if (p.room_id) {
      await this.#setChatRoomTyping(p.room_id, true, '👀 reading context');
    }

    const delegationEnabled = this.#config?.delegation?.enabled !== false;
    const persistentChat = this.#config?.delegation?.persistentChatSessions !== false;

    if (delegationEnabled && persistentChat && this.#chatSessionManager && p.room_id) {
      try {
        await this.#setChatRoomTyping(p.room_id, true, 'thinking');
        const result = await this.#chatSessionManager.dispatch({
          roomId: p.room_id,
          senderId: p.sender_id || '',
          senderName: p.sender_name || '',
          createdAt: p.created_at || '',
          content: p.content || '',
          rolePrompt: p.role_prompt || '',
        });
        // Record into ring AFTER dispatch so the spawn path sees real prior
        // history (or empty → REST fallback) rather than self-referencing
        // the very message that triggered the dispatch.
        this.#chatSessionManager?.recordRoomMessage(p);
        if (result.dispatched) {
          await this.#setChatRoomTyping(p.room_id, true, 'composing reply');
          log(`Chat room message dispatched to session: room=${p.room_id} pid=${result.pid}${result.firstTurn ? ' (new session)' : ''}`);
          return;
        }
        if (result.reason === 'duplicate_chat') {
          log(`Chat room message deduped: room=${p.room_id} sender=${p.sender_id} ts=${p.created_at || ''}`);
          return;
        }
        log(`Chat room session dispatch declined (${result.reason}), falling back to legacy path`);
      } catch (err) {
        log(`Chat room session path failed: ${err.message}, falling back to legacy path`);
      }
    }

    const canDelegate = delegationEnabled && this.#subagentManager && this.#subagentManager.canSpawn();

    if (canDelegate) {
      try {
        await this.#setChatRoomTyping(p.room_id, true, 'thinking');
        const history = await fetchChatRoomHistory(this.#config, p.room_id);
        const rolePrompt = p.role_prompt || '';
        const taskText = composeChatRoomPrompt(p.room_id, history, {
          content: p.content || '',
          sender_name: p.sender_name || '',
          sender_id: p.sender_id || '',
        });

        const result = await this.#subagentManager.spawn({
          kind: 'chat',
          taskText,
          rolePrompt,
          chatRequestId: `msg:${p.sender_id}:${p.created_at || ''}`,
          ticketId: '',
          agentId: '',
        });

        if (result.spawned) {
          await this.#setChatRoomTyping(p.room_id, true, 'composing reply');
          log(`Chat room message dispatched to subagent: room=${p.room_id} pid=${result.pid}`);
          return;
        }
        log(`Chat room subagent spawn declined (${result.reason}), falling back to main session forward`);
      } catch (err) {
        log(`Chat room delegation path failed: ${err.message}, falling back to main session forward`);
      }
    }

    // ── Fallback: notify main session ───────────────────────────────
    sendChannelEvent(
      `[AWB Chat] room=${p.room_id} from=${p.sender_name || p.sender_id} "${(p.content || '').slice(0, 80)}"`,
      {
        type: 'chat_room_message',
        room_id: p.room_id || '',
        sender_type: p.sender_type || '',
        sender_id: p.sender_id || '',
        sender_name: p.sender_name || '',
        content: p.content || '',
        timestamp: p.created_at || new Date().toISOString(),
      },
    );
    log(`Chat room message forwarded (fallback path): room=${p.room_id} sender=${p.sender_name || p.sender_id}`);
  }

  #scheduleReconnect() {
    if (this.#stopped) return;
    setTimeout(() => this.#connect(), this.#retryDelay);
    this.#retryDelay = Math.min(this.#retryDelay * 1.5, RECONNECT_MAX_MS);
  }

  // Test-only accessor — only exposed when AWB_TEST_MODE is set. Lets Plan 04-04
  // integration tests invoke the private handlers without opening a real SSE stream.
  _testDispatchTrigger(raw) { return this.#handleTrigger(raw); }
  _testDispatchChatRequest(raw) { return this.#handleChatRequest(raw); }
  _testDispatchChatRoomMessage(raw) { return this.#handleChatRoomMessage(raw); }
}
