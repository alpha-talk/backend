package com.alphatalk.worker.batch.master

import com.alphatalk.kis.master.KisSector
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.EntityManager
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional

@Entity
@Table(name = "sector")
class SectorEntity(
    @Id
    @Column(name = "code", nullable = false)
    val code: String = "",
    @Column(name = "name", nullable = false)
    var name: String = "",
)

interface SectorJpaRepository : JpaRepository<SectorEntity, String>

@Repository
class JpaSectorStore(
    private val repository: SectorJpaRepository,
    private val entityManager: EntityManager,
) : SectorStore {
    @Transactional
    override fun upsertAll(sectors: List<KisSector>): Int {
        if (sectors.isEmpty()) return 0
        val existing = repository.findAllById(sectors.map(KisSector::code)).associateBy(SectorEntity::code)
        sectors.forEach { sector ->
            existing[sector.code]?.apply { name = sector.name }
                ?: entityManager.persist(SectorEntity(code = sector.code, name = sector.name))
        }
        return sectors.size
    }
}
