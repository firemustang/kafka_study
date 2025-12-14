package org.kafka_lecture.controller


import org.kafka_lecture.model.*
import org.kafka_lecture.stream.OrderStreamsService
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/stats")
class StatsController(
    private val orderStreamsService: OrderStreamsService
) {

    @GetMapping("/orders/count")
    fun getOrderCountStats(): ResponseEntity<StatsResponse<OrderCountComparisonStats>> {
        val stats = orderStreamsService.orderCountComparison()
        val response = StatsResponse(
            success = true,
            data = stats,
            message = "Order count statistics retrieved successfully"
        )
        return ResponseEntity.ok(response)
    }
}