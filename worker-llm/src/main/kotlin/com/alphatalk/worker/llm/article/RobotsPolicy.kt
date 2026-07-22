package com.alphatalk.worker.llm.article

import java.net.HttpURLConnection
import java.net.URI
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

class RobotsPolicy(
    private val fetch: (URI) -> FetchResult = ::fetchRobotsTxt,
    private val ttl: Duration = Duration.ofHours(6),
    private val failureTtl: Duration = Duration.ofMinutes(5),
    private val clock: () -> Long = System::currentTimeMillis,
    private val gate: ArticleRequestGate = ArticleRequestGate { },
) {
    sealed interface FetchResult {
        data class Ok(val body: String) : FetchResult
        data object Unavailable : FetchResult
    }

    private data class Rule(val pattern: Regex, val length: Int, val allow: Boolean)
    private data class CacheEntry(val rules: List<Rule>, val expiresAt: Long)

    private val cache = ConcurrentHashMap<String, CacheEntry>()

    fun allowed(uri: URI): Boolean {
        val origin = "${uri.scheme}://${uri.authority}"
        val entry = cache[origin]?.takeIf { it.expiresAt > clock() } ?: load(origin)
        val path = uri.rawPath?.ifEmpty { "/" }?.let { p ->
            if (uri.rawQuery != null) "$p?${uri.rawQuery}" else p
        } ?: "/"
        val match = entry.rules
            .filter { it.pattern.containsMatchIn(path) }
            .maxWithOrNull(compareBy<Rule> { it.length }.thenBy { it.allow })
        return match?.allow ?: true
    }

    private fun load(origin: String): CacheEntry {
        val robotsUri = URI("$origin/robots.txt")
        gate.await(robotsUri)
        val entry = when (val result = fetch(robotsUri)) {
            is FetchResult.Ok -> CacheEntry(parse(result.body), clock() + ttl.toMillis())
            FetchResult.Unavailable -> CacheEntry(emptyList(), clock() + failureTtl.toMillis())
        }
        cache[origin] = entry
        return entry
    }

    private fun parse(body: String): List<Rule> {
        val rules = mutableListOf<Rule>()
        var applies = false
        var afterRule = false
        for (line in body.lineSequence()) {
            val content = line.substringBefore('#').trim()
            if (content.isEmpty()) continue
            val key = content.substringBefore(':').trim().lowercase()
            val value = content.substringAfter(':', "").trim()
            when (key) {
                "user-agent" -> {
                    if (afterRule) {
                        applies = false
                        afterRule = false
                    }
                    applies = applies || value == "*" || AGENT_TOKEN.startsWith(value.lowercase())
                }
                "disallow", "allow" -> {
                    afterRule = true
                    if (applies && value.isNotEmpty()) {
                        rules.add(Rule(toRegex(value), value.length, allow = key == "allow"))
                    }
                }
                else -> afterRule = true
            }
        }
        return rules
    }

    private fun toRegex(path: String): Regex {
        val builder = StringBuilder("^")
        var i = 0
        while (i < path.length) {
            when (val c = path[i]) {
                '*' -> builder.append(".*")
                '$' -> if (i == path.length - 1) builder.append('$') else builder.append("\\$")
                else -> builder.append(Regex.escape(c.toString()))
            }
            i++
        }
        return Regex(builder.toString())
    }

    companion object {
        private const val AGENT_TOKEN = "alphatalkllm"
    }
}

private fun fetchRobotsTxt(uri: URI): RobotsPolicy.FetchResult = runCatching {
    val connection = uri.toURL().openConnection() as HttpURLConnection
    connection.connectTimeout = 3_000
    connection.readTimeout = 5_000
    connection.instanceFollowRedirects = false
    connection.setRequestProperty("User-Agent", "AlphaTalkLlm/0.1")
    when (connection.responseCode) {
        200 -> RobotsPolicy.FetchResult.Ok(
            connection.inputStream.use { String(it.readNBytes(100_000), Charsets.UTF_8) },
        )
        in 400..499 -> RobotsPolicy.FetchResult.Ok("")
        else -> RobotsPolicy.FetchResult.Unavailable
    }
}.getOrDefault(RobotsPolicy.FetchResult.Unavailable)
