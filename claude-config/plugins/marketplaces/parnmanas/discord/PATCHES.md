# Discord Plugin Patches

This is a forked copy of `discord@claude-plugins-official` (v0.0.4).
When the upstream plugin is updated, re-apply the patches documented below.

## Setup: Symlink to replace marketplace cache

The `--channels plugin:discord@claude-plugins-official` option only accepts marketplace references.
To use this fork, the marketplace cache is symlinked to this directory:

```
~/.claude/plugins/cache/claude-plugins-official/discord/0.0.4
  → /mnt/data/repositories/ralf/claude-plugins/discord  (symlink)

~/.claude/plugins/cache/claude-plugins-official/discord/0.0.4.original
  → original backup (restore if needed)
```

**Launch command is unchanged:**
```bash
claude --channels plugin:discord@claude-plugins-official
```

**If the upstream plugin updates (e.g. to 0.0.5):**
1. Check the new version: `ls ~/.claude/plugins/cache/claude-plugins-official/discord/`
2. Diff against this fork: compare new `server.ts` with `0.0.4.original/server.ts`
3. Re-apply patches from below to the new version
4. Update the symlink: `ln -sfn /mnt/data/repositories/ralf/claude-plugins/discord ~/.claude/plugins/cache/claude-plugins-official/discord/<new-version>`
5. Back up the new original: `mv <new-version> <new-version>.original` (before symlinking)

---

## Patch 1: `allowBots` — Accept messages from other bots

**Date:** 2026-04-08
**Problem:** `messageCreate` handler unconditionally drops all bot messages (`msg.author.bot` check at line 803), making bot-to-bot mention communication impossible.
**Solution:** Added `allowBots` option to `access.json`, controllable at global and per-group level.

### Changes in `server.ts`

#### 1. `GroupPolicy` type — added `allowBots` field
```typescript
type GroupPolicy = {
  requireMention: boolean
  allowFrom: string[]
  allowBots?: boolean    // +added
}
```

#### 2. `Access` type — added `allowBots` field
```typescript
type Access = {
  // ...existing fields...
  allowBots?: boolean    // +added
}
```

#### 3. `readAccessFile()` — parse `allowBots` from JSON
```typescript
// added to the return object:
allowBots: parsed.allowBots,
```

#### 4. `messageCreate` handler — conditional bot filtering (main change)

**Before:**
```typescript
client.on('messageCreate', msg => {
  if (msg.author.bot) return
  handleInbound(msg).catch(...)
})
```

**After:**
```typescript
client.on('messageCreate', msg => {
  if (msg.author.id === client.user?.id) return
  if (msg.author.bot) {
    const access = loadAccess()
    const isDM = msg.channel.type === ChannelType.DM
    if (isDM) {
      if (!access.allowBots) return
    } else {
      const channelId = msg.channel.isThread?.()
        ? (msg.channel as any).parentId ?? msg.channelId
        : msg.channelId
      const groupPolicy = access.groups[channelId]
      if (!groupPolicy?.allowBots && !access.allowBots) return
    }
  }
  handleInbound(msg).catch(...)
})
```

### `access.json` configuration

```json
{
  "allowBots": true,
  "groups": {
    "<channel_id>": {
      "requireMention": true,
      "allowFrom": [],
      "allowBots": true
    }
  }
}
```

- **Global `allowBots`**: `true` = allow bot messages in DMs and as fallback for groups
- **Per-group `allowBots`**: overrides global for that specific channel
- **Default**: `false` (original behavior preserved)
- Self-messages (own bot ID) are always ignored regardless of setting
