// ─── SSE Event Stream ─────────────────────────────────────

import { RECONNECT_INITIAL_MS, RECONNECT_MAX_MS } from './constants.mjs';
import { log } from './logging.mjs';
import { EventDispatcher } from './event-dispatcher.mjs';

/**
 * Connect to AWB's SSE /api/events/stream and forward ticket events
 * as claude/channel notifications. Reconnects with exponential backoff.
 *
 * AWB SSE event types (from events.controller.ts):
 *   - board_update:  ticket/comment CRUD (entity_type, action, field_changed, actor_name)
 *   - agent_trigger: trigger assigned to agent (role, trigger_id, agent_id)
 *   - agent_typing:  typing indicator (ignored by proxy)
 *
 * Responsibilities are narrow by design:
 *   - HTTP fetch + AbortController + reconnect backoff
 *   - SSE line parsing (event: / data: / blank-line terminator)
 *   - Handing each (eventType, raw) pair to the injected EventDispatcher
 *
 * Dispatch decisions (persistent session vs one-shot subagent vs legacy
 * main-session forward) live in EventDispatcher — see lib/event-dispatcher.mjs.
 */
export class EventStream {
  #url;
  #retryDelay = RECONNECT_INITIAL_MS;
  #abortController = null;
  #stopped = false;
  #dispatcher;

  /**
   * Back-compat constructor — accepts the legacy 4-arg form so proxy.mjs and
   * existing tests keep working without modification. Internally builds an
   * EventDispatcher from the three managers.
   */
  constructor(config, subagentManager = null, chatSessionManager = null, ticketSessionManager = null) {
    this.#url = `${config.url.replace(/\/$/, '')}/api/events/stream?token=${encodeURIComponent(config.apiKey)}`;
    this.#dispatcher = new EventDispatcher(config, {
      subagentManager,
      chatSessionManager,
      ticketSessionManager,
    });
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
          if (data) this.#dispatcher.dispatch(eventType, data);
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

  #scheduleReconnect() {
    if (this.#stopped) return;
    setTimeout(() => this.#connect(), this.#retryDelay);
    this.#retryDelay = Math.min(this.#retryDelay * 1.5, RECONNECT_MAX_MS);
  }

  // Test-only accessors — only exposed when AWB_TEST_MODE is set. Lets Plan 04-04
  // integration tests invoke the private handlers without opening a real SSE stream.
  _testDispatchTrigger(raw) { return this.#dispatcher.handleTrigger(raw); }
  _testDispatchChatRequest(raw) { return this.#dispatcher.handleChatRequest(raw); }
  _testDispatchChatRoomMessage(raw) { return this.#dispatcher.handleChatRoomMessage(raw); }
}
