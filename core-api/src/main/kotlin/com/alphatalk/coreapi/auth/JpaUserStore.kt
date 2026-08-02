package com.alphatalk.coreapi.auth

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.LockModeType
import jakarta.persistence.Table
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.data.jpa.repository.Lock
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
import java.sql.SQLException
import java.time.Instant

@Entity
@Table(name = "users")
class UserEntity(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    var id: Long? = null,
    @Column(nullable = false, unique = true)
    var email: String = "",
    @Column(name = "password_hash", nullable = false)
    var passwordHash: String = "",
    @Column(nullable = false, unique = true)
    var nickname: String = "",
    @Column(name = "created_at", nullable = false, insertable = false, updatable = false)
    var createdAt: Instant? = null,
)

interface UserNicknameView {
    val id: Long
    val nickname: String
}

interface UserJpaRepository : JpaRepository<UserEntity, Long> {
    fun findByEmail(email: String): UserEntity?
    fun existsByEmail(email: String): Boolean
    fun existsByNickname(nickname: String): Boolean
    fun findByIdIn(ids: Collection<Long>): List<UserNicknameView>

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select u from UserEntity u where u.id = :userId")
    fun findByIdForUpdate(@Param("userId") userId: Long): UserEntity?
}

@Repository
class JpaUserStore(
    private val users: UserJpaRepository,
) : UserStore {
    override fun create(email: String, passwordHash: String, nickname: String): Long = try {
        users.saveAndFlush(
            UserEntity(
                email = email,
                passwordHash = passwordHash,
                nickname = nickname,
            ),
        ).id ?: error("persisted user id is missing")
    } catch (e: DataIntegrityViolationException) {
        if ((e.mostSpecificCause as? SQLException)?.sqlState != UNIQUE_VIOLATION_SQL_STATE) throw e
        throw DuplicateUserException(e)
    }

    override fun findByEmail(email: String): UserRecord? =
        users.findByEmail(email)?.toRecord()

    override fun findById(id: Long): UserRecord? =
        users.findById(id).orElse(null)?.toRecord()

    override fun existsByEmail(email: String): Boolean =
        users.existsByEmail(email)

    override fun existsByNickname(nickname: String): Boolean =
        users.existsByNickname(nickname)

    override fun nicknames(ids: Collection<Long>): Map<Long, String> {
        if (ids.isEmpty()) return emptyMap()
        return users.findByIdIn(ids).associate { it.id to it.nickname }
    }

    private fun UserEntity.toRecord() = UserRecord(
        id = requireNotNull(id),
        email = email,
        passwordHash = passwordHash,
        nickname = nickname,
        createdAt = requireNotNull(createdAt),
    )

    companion object {
        private const val UNIQUE_VIOLATION_SQL_STATE = "23505"
    }
}

@Repository
class JpaUserAccountLock(
    private val users: UserJpaRepository,
) : UserAccountLock {
    override fun acquire(userId: Long): Boolean =
        users.findByIdForUpdate(userId) != null
}
