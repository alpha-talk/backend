package com.alphatalk.worker.batch.industry

import com.alphatalk.worker.batch.master.StockMasterEntity
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.EntityManager
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional
import java.time.Clock
import java.time.Instant

@Entity
@Table(name = "industry")
class IndustryEntity(
    @Id
    @Column(name = "code", nullable = false)
    val code: String = "",
    @Column(name = "name", nullable = false)
    var name: String = "",
    @Column(name = "level", nullable = false)
    var level: Short = 0,
)

@Entity
@Table(name = "dart_corp_map")
class DartCorpMapEntity(
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "corp_code", nullable = false, length = 8)
    val corpCode: String = "",
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "code", length = 6)
    var code: String? = null,
    @Column(name = "corp_name", nullable = false)
    var corpName: String = "",
    @Column(name = "updated_at", nullable = false)
    var updatedAt: Instant = Instant.EPOCH,
)

@Entity
@Table(name = "stock_industry")
class StockIndustryEntity(
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "code", nullable = false, length = 6)
    val code: String = "",
    @Column(name = "induty_code", nullable = false)
    var indutyCode: String = "",
    @Column(name = "group_code", nullable = false)
    var groupCode: String = "",
    @Column(name = "corp_name")
    var corpName: String? = null,
    @Column(name = "corp_name_eng")
    var corpNameEng: String? = null,
    @Column(name = "stock_name")
    var stockName: String? = null,
    @Column(name = "homepage")
    var homepage: String? = null,
    @Column(name = "updated_at", nullable = false)
    var updatedAt: Instant = Instant.EPOCH,
)

interface IndustryJpaRepository : JpaRepository<IndustryEntity, String>

interface DartCorpMapJpaRepository : JpaRepository<DartCorpMapEntity, String>

interface StockIndustryJpaRepository : JpaRepository<StockIndustryEntity, String>

interface ActiveStockCodeRepository : JpaRepository<StockMasterEntity, String> {
    @Query("select trim(s.code) from StockMasterEntity s where s.isActive = true")
    fun activeCodes(): List<String>
}

@Repository
class JpaIndustryStore(
    private val industries: IndustryJpaRepository,
    private val corpMaps: DartCorpMapJpaRepository,
    private val stockIndustries: StockIndustryJpaRepository,
    private val activeStocks: ActiveStockCodeRepository,
    private val entityManager: EntityManager,
    private val clock: Clock = Clock.systemUTC(),
) : IndustryStore {

    @Transactional
    override fun upsertIndustries(entries: List<KsicEntry>): Int {
        if (entries.isEmpty()) return 0
        val existing = industries.findAllById(entries.map(KsicEntry::code)).associateBy(IndustryEntity::code)
        entries.forEach { entry ->
            val entity = existing[entry.code]
            if (entity == null) {
                entityManager.persist(
                    IndustryEntity(code = entry.code, name = entry.name, level = entry.level.toShort()),
                )
            } else {
                entity.name = entry.name
                entity.level = entry.level.toShort()
            }
        }
        return entries.size
    }

    @Transactional
    override fun upsertCorpMap(corps: List<DartCorp>): Int {
        if (corps.isEmpty()) return 0
        val now = clock.instant()
        val existing = corpMaps.findAllById(corps.map(DartCorp::corpCode)).associateBy { it.corpCode.trim() }
        corps.forEach { corp ->
            val entity = existing[corp.corpCode]
            if (entity == null) {
                entityManager.persist(
                    DartCorpMapEntity(
                        corpCode = corp.corpCode,
                        code = corp.stockCode,
                        corpName = corp.corpName,
                        updatedAt = now,
                    ),
                )
            } else {
                entity.code = corp.stockCode
                entity.corpName = corp.corpName
                entity.updatedAt = now
            }
        }
        return corps.size
    }

    @Transactional
    override fun upsertStockIndustries(records: List<StockIndustryRecord>): Int {
        if (records.isEmpty()) return 0
        val now = clock.instant()
        val existing = stockIndustries.findAllById(records.map(StockIndustryRecord::code))
            .associateBy { it.code.trim() }
        records.forEach { record ->
            val entity = existing[record.code]
            if (entity == null) {
                entityManager.persist(
                    StockIndustryEntity(
                        code = record.code,
                        indutyCode = record.indutyCode,
                        groupCode = record.groupCode,
                        corpName = record.corpName,
                        corpNameEng = record.corpNameEng,
                        stockName = record.stockName,
                        homepage = record.homepage,
                        updatedAt = now,
                    ),
                )
            } else {
                entity.apply {
                    indutyCode = record.indutyCode
                    groupCode = record.groupCode
                    corpName = record.corpName
                    corpNameEng = record.corpNameEng
                    stockName = record.stockName
                    homepage = record.homepage
                    updatedAt = now
                }
            }
        }
        return records.size
    }

    @Transactional(readOnly = true)
    override fun activeStockCodes(): Set<String> = activeStocks.activeCodes().toSet()
}
