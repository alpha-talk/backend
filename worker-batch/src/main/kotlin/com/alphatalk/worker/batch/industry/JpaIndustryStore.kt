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
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional
import java.time.Clock
import java.time.Instant

@Entity
@Table(name = "sector")
class SectorCatalogEntity(
    @Id
    @Column(name = "code", nullable = false)
    val code: String = "",
    @Column(name = "name", nullable = false)
    var name: String = "",
    @Column(name = "level", nullable = false)
    var level: Short = 0,
    @Column(name = "parent_code")
    var parentCode: String? = null,
    @Column(name = "version", nullable = false)
    var version: String = KsicCatalog.VERSION,
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
    @Column(name = "modify_date")
    var modifyDate: String? = null,
    @Column(name = "updated_at", nullable = false)
    var updatedAt: Instant = Instant.EPOCH,
)

interface SectorCatalogJpaRepository : JpaRepository<SectorCatalogEntity, String>

interface DartCorpMapJpaRepository : JpaRepository<DartCorpMapEntity, String>

interface IndustryStockJpaRepository : JpaRepository<StockMasterEntity, String> {
    @Query("select trim(s.code) from StockMasterEntity s where s.isActive = true")
    fun activeCodes(): List<String>

    @Query(
        """
        select trim(s.code), s.dartIndutyCode from StockMasterEntity s
        where s.isActive = true and s.dartIndutyCode is not null
        """,
    )
    fun activeIndutyRows(): List<Array<Any>>

    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query(
        """
        update StockMasterEntity s set s.sectorCode = null, s.dartIndutyCode = null
        where s.code in :codes and (s.sectorCode is not null or s.dartIndutyCode is not null)
        """,
    )
    fun clearIndustry(@Param("codes") codes: Collection<String>): Int
}

@Repository
class JpaIndustryStore(
    private val sectors: SectorCatalogJpaRepository,
    private val corpMaps: DartCorpMapJpaRepository,
    private val stocks: IndustryStockJpaRepository,
    private val entityManager: EntityManager,
    private val clock: Clock = Clock.systemUTC(),
) : IndustryStore {

    @Transactional
    override fun upsertSectors(entries: List<KsicEntry>): Int {
        if (entries.isEmpty()) return 0
        val existing = sectors.findAllById(entries.map(KsicEntry::code)).associateBy(SectorCatalogEntity::code)
        entries.forEach { entry ->
            val entity = existing[entry.code]
            if (entity == null) {
                entityManager.persist(
                    SectorCatalogEntity(
                        code = entry.code,
                        name = entry.name,
                        level = entry.level.toShort(),
                        parentCode = entry.parentCode,
                    ),
                )
            } else {
                entity.name = entry.name
                entity.level = entry.level.toShort()
                entity.parentCode = entry.parentCode
                entity.version = KsicCatalog.VERSION
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
                        modifyDate = corp.modifyDate,
                        updatedAt = now,
                    ),
                )
            } else {
                entity.code = corp.stockCode
                entity.corpName = corp.corpName
                entity.modifyDate = corp.modifyDate
                entity.updatedAt = now
            }
        }
        return corps.size
    }

    @Transactional
    override fun upsertStockIndustries(records: List<StockIndustryRecord>): Int {
        if (records.isEmpty()) return 0
        val now = clock.instant()
        val existing = stocks.findAllById(records.map(StockIndustryRecord::code)).associateBy { it.code.trim() }
        records.forEach { record ->
            val entity = existing[record.code] ?: return@forEach
            entity.dartIndutyCode = record.indutyCode
            entity.sectorCode = record.sectorCode
            entity.updatedAt = now
        }
        return records.count { existing.containsKey(it.code) }
    }

    @Transactional
    override fun clearIndustryAssignments(codes: Collection<String>): Int =
        if (codes.isEmpty()) 0 else stocks.clearIndustry(codes)

    @Transactional(readOnly = true)
    override fun activeStockCodes(): Set<String> = stocks.activeCodes().toSet()

    @Transactional(readOnly = true)
    override fun activeIndutyCodes(): Map<String, String> =
        stocks.activeIndutyRows().associate { it[0] as String to it[1] as String }
}
