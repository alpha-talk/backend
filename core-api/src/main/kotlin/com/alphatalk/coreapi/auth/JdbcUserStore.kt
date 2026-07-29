package com.alphatalk.coreapi.auth

import org.springframework.dao.DuplicateKeyException
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Repository
import java.sql.ResultSet

@Repository
class JdbcUserStore(
    private val jdbc: JdbcTemplate,
) : UserStore {
    override fun create(email: String, passwordHash: String, nickname: String): Long = try {
        jdbc.query(
            "INSERT INTO users (email, password_hash, nickname) VALUES (?, ?, ?) RETURNING id",
            { rows, _ -> rows.getLong("id") },
            email,
            passwordHash,
            nickname,
        ).first()
    } catch (e: DuplicateKeyException) {
        throw DuplicateUserException(e)
    }

    override fun findByEmail(email: String): UserRecord? =
        jdbc.query("$SELECT_USER WHERE email = ?", ::mapUser, email).firstOrNull()

    override fun findById(id: Long): UserRecord? =
        jdbc.query("$SELECT_USER WHERE id = ?", ::mapUser, id).firstOrNull()

    override fun existsByEmail(email: String): Boolean =
        jdbc.queryForObject("SELECT exists(SELECT 1 FROM users WHERE email = ?)", Boolean::class.java, email) == true

    override fun existsByNickname(nickname: String): Boolean =
        jdbc.queryForObject("SELECT exists(SELECT 1 FROM users WHERE nickname = ?)", Boolean::class.java, nickname) == true

    private fun mapUser(rows: ResultSet, rowNum: Int) = UserRecord(
        id = rows.getLong("id"),
        email = rows.getString("email"),
        passwordHash = rows.getString("password_hash"),
        nickname = rows.getString("nickname"),
        createdAt = rows.getTimestamp("created_at").toInstant(),
    )

    companion object {
        private const val SELECT_USER = "SELECT id, email, password_hash, nickname, created_at FROM users"
    }
}

class DuplicateUserException(cause: Throwable) : RuntimeException(cause)
