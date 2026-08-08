package com.alphatalk.worker.llm.enrich

import com.alphatalk.worker.llm.config.LlmProperties
import com.alphatalk.worker.llm.sector.SectorJpaRepository
import com.alphatalk.worker.llm.sector.StockMasterEntity
import com.alphatalk.worker.llm.sector.StockMasterJpaRepository
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.IdClass
import jakarta.persistence.Table
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.springframework.data.domain.PageRequest
import org.springframework.data.domain.Pageable
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
import java.io.Serializable
import java.time.LocalDate
import java.time.format.DateTimeFormatter

data class DailyRowId(var code: String = "", var date: String = "") : Serializable

@Entity
@Table(name = "daily_candle")
@IdClass(DailyRowId::class)
class DailyCandleReadEntity(
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "code", nullable = false, length = 6)
    var code: String = "",
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "date", nullable = false, length = 8)
    var date: String = "",
    @Column(name = "close", nullable = false)
    var close: Int = 0,
)

@Entity
@Table(name = "investor_flow_daily")
@IdClass(DailyRowId::class)
class InvestorFlowReadEntity(
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "code", nullable = false, length = 6)
    var code: String = "",
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "date", nullable = false, length = 8)
    var date: String = "",
    @Column(name = "\"foreign\"", nullable = false)
    var foreignNet: Long = 0,
    @Column(name = "institution", nullable = false)
    var institutionNet: Long = 0,
)

interface DailyCandleReadRepository : JpaRepository<DailyCandleReadEntity, DailyRowId> {
    @Query("select distinct c.date from DailyCandleReadEntity c where c.date <= :date order by c.date desc")
    fun findRecentDates(@Param("date") date: String, pageable: Pageable): List<String>

    fun findByDate(date: String): List<DailyCandleReadEntity>
}

interface InvestorFlowReadRepository : JpaRepository<InvestorFlowReadEntity, DailyRowId> {
    fun findByDate(date: String): List<InvestorFlowReadEntity>
}

@Repository
class JpaMarketFactSheetSource(
    private val candles: DailyCandleReadRepository,
    private val flows: InvestorFlowReadRepository,
    private val stocks: StockMasterJpaRepository,
    private val sectors: SectorJpaRepository,
    private val props: LlmProperties,
) : MarketFactSheetSource {

    override fun lookup(onOrBefore: LocalDate): FactSheetLookup {
        val activeStocks = stocks.findByIsActiveTrue()
        if (activeStocks.isEmpty()) return FactSheetLookup.Missing
        val activeCodes = activeStocks.map { it.code.trim() }.toSet()
        val required = activeCodes.size * props.market.factCoverageThreshold
        val dates = candles.findRecentDates(onOrBefore.format(COMPACT), PageRequest.of(0, LOOKBACK_DATES))
        for ((index, date) in dates.withIndex()) {
            val flowRows = flows.findByDate(date).filter { it.code.trim() in activeCodes }
            if (flowRows.isEmpty()) continue
            val currentRows = candles.findByDate(date).filter { it.code.trim() in activeCodes }
            if (flowRows.size < required || currentRows.size < required) return FactSheetLookup.Insufficient
            val prevDate = dates.getOrNull(index + 1) ?: return FactSheetLookup.Insufficient
            val prevRows = candles.findByDate(prevDate).filter { it.code.trim() in activeCodes }
            val comparable = currentRows.map { it.code.trim() }.toSet()
                .intersect(prevRows.map { it.code.trim() }.toSet())
            if (comparable.size < required) return FactSheetLookup.Insufficient
            val sheet = MarketFactSheetAssembler.assemble(
                date = LocalDate.parse(date, COMPACT).toString(),
                activeStocks = activeStocks,
                sectorNames = sectors.findAll().associate { it.code to it.name },
                currentCloses = currentRows.associateBy({ it.code.trim() }, { it.close }),
                previousCloses = prevRows.associateBy({ it.code.trim() }, { it.close }),
                flowRows = flowRows,
            )
            return FactSheetLookup.Found(sheet)
        }
        return FactSheetLookup.Missing
    }

    private companion object {
        val COMPACT: DateTimeFormatter = DateTimeFormatter.BASIC_ISO_DATE
        const val LOOKBACK_DATES = 10
    }
}

internal object MarketFactSheetAssembler {
    private const val TOP_N = 5

    fun assemble(
        date: String,
        activeStocks: List<StockMasterEntity>,
        sectorNames: Map<String, String>,
        currentCloses: Map<String, Int>,
        previousCloses: Map<String, Int>,
        flowRows: List<InvestorFlowReadEntity>,
    ): MarketFactSheet {
        var advancers = 0
        var decliners = 0
        var unchanged = 0
        val sectorChanges = mutableMapOf<String, MutableList<Double>>()
        for (stock in activeStocks) {
            val code = stock.code.trim()
            val close = currentCloses[code] ?: continue
            val prevClose = previousCloses[code]?.takeIf { it > 0 } ?: continue
            when {
                close > prevClose -> advancers++
                close < prevClose -> decliners++
                else -> unchanged++
            }
            val changePct = (close - prevClose) * 100.0 / prevClose
            stock.sectorCode?.let { sectorChanges.getOrPut(it) { mutableListOf() }.add(changePct) }
        }

        val performances = sectorChanges.mapNotNull { (sectorCode, changes) ->
            val name = sectorNames[sectorCode] ?: return@mapNotNull null
            MarketFactSheet.SectorPerformance(name = name, avgChangePct = changes.average(), stockCount = changes.size)
        }

        val sectorOf = activeStocks.associateBy({ it.code.trim() }, { it.sectorCode })
        val foreignBySector = mutableMapOf<String, Long>()
        val institutionBySector = mutableMapOf<String, Long>()
        for (flow in flowRows) {
            val name = sectorOf[flow.code.trim()]?.let(sectorNames::get) ?: continue
            foreignBySector.merge(name, flow.foreignNet, Long::plus)
            institutionBySector.merge(name, flow.institutionNet, Long::plus)
        }

        return MarketFactSheet(
            factDate = date,
            advancers = advancers,
            decliners = decliners,
            unchanged = unchanged,
            topSectors = performances.sortedByDescending { it.avgChangePct }.take(TOP_N),
            bottomSectors = performances.sortedBy { it.avgChangePct }.take(TOP_N),
            foreignNetBuyTop = topFlows(foreignBySector),
            institutionNetBuyTop = topFlows(institutionBySector),
        )
    }

    private fun topFlows(bySector: Map<String, Long>): List<MarketFactSheet.SectorFlow> =
        bySector.entries
            .sortedByDescending { it.value }
            .take(TOP_N)
            .map { MarketFactSheet.SectorFlow(name = it.key, netBuy = it.value) }
}
