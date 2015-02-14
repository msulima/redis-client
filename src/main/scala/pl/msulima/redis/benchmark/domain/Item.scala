package pl.msulima.redis.benchmark.domain

import java.time.Instant

case class Item(id: String, name: String, price: BigDecimal, sellerId: String, bidCount: Int,
                imageUrl: Option[String], startDate: Instant, endDate: Instant)
