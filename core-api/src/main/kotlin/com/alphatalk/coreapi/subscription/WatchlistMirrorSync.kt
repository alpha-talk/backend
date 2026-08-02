package com.alphatalk.coreapi.subscription

import com.alphatalk.contracts.Channels
import com.alphatalk.contracts.Keys
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.script.DefaultRedisScript
import org.springframework.stereotype.Component
import java.time.Clock

data class MirrorChange(
    val state: WatchlistState,
    val addedCode: String? = null,
    val removedCode: String? = null,
)

interface WatchlistMirrorSync {
    fun sync(userId: Long, change: MirrorChange): Boolean
}

@Component
class RedisWatchlistMirrorSync(
    private val redis: StringRedisTemplate,
    private val clock: Clock = Clock.systemUTC(),
) : WatchlistMirrorSync {
    override fun sync(userId: Long, change: MirrorChange): Boolean {
        val requestKind = if (change.addedCode != null) "added" else "removed"
        val requestCode = change.addedCode ?: requireNotNull(change.removedCode)
        val args = buildList {
            add(change.state.rev.toString())
            add(Channels.WATCHLIST_UPDATED)
            add(userId.toString())
            add(clock.millis().toString())
            add(requestKind)
            add(requestCode)
            addAll(change.state.codes)
        }
        val applied = redis.execute(
            REPLACE_IF_NEWER,
            listOf(Keys.watchlist(userId), Keys.watchlistRev(userId)),
            *args.toTypedArray(),
        )
        return applied == 1L
    }

    companion object {
        private val REPLACE_IF_NEWER = DefaultRedisScript(
            """
            local rev = tonumber(ARGV[1])
            local cur = tonumber(redis.call('GET', KEYS[2]) or '-1')
            if rev <= cur then return 0 end
            redis.call('SET', KEYS[2], ARGV[1])
            local old = redis.call('SMEMBERS', KEYS[1])
            local oldset = {}
            for _, c in ipairs(old) do oldset[c] = true end
            redis.call('DEL', KEYS[1])
            local newset = {}
            local added = {}
            for i = 7, #ARGV do
              local c = ARGV[i]
              redis.call('SADD', KEYS[1], c)
              if not newset[c] then
                newset[c] = true
                if not oldset[c] then added[#added + 1] = c end
              end
            end
            local removed = {}
            for _, c in ipairs(old) do
              if not newset[c] then removed[#removed + 1] = c end
            end
            local function contains(list, value)
              for _, x in ipairs(list) do
                if x == value then return true end
              end
              return false
            end
            if ARGV[5] == 'added' and not contains(added, ARGV[6]) then added[#added + 1] = ARGV[6] end
            if ARGV[5] == 'removed' and not contains(removed, ARGV[6]) then removed[#removed + 1] = ARGV[6] end
            if #added == 0 and #removed == 0 then return 1 end
            local function jsonArray(list)
              if #list == 0 then return '[]' end
              local parts = {}
              for i, v in ipairs(list) do parts[i] = '"' .. v .. '"' end
              return '[' .. table.concat(parts, ',') .. ']'
            end
            local payload = '{"userId":' .. ARGV[3] .. ',"added":' .. jsonArray(added)
              .. ',"removed":' .. jsonArray(removed) .. ',"ts":' .. ARGV[4] .. '}'
            redis.call('PUBLISH', ARGV[2], payload)
            return 1
            """.trimIndent(),
            Long::class.javaObjectType,
        )
    }
}
