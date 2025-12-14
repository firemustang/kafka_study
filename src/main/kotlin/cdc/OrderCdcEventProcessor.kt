package org.kafka_lecture.cdc

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

@Component
class OrderCdcEventProcessor(
    private val objectMapper: ObjectMapper
) {

    private val logger = LoggerFactory.getLogger(OrderCdcEventProcessor::class.java)

    @KafkaListener(
        topics = ["dbserver1.public.orders"],
        groupId = "real-order-cdc-processor",
        containerFactory = "cdcKafkaListenerContainerFactory"
    )
    fun processRealCdcEvent(
        @Payload cdcMessage: String
    ) {
        try {
            logger.info("Raw CDC message: {}", cdcMessage)
            val cdcEvent = objectMapper.readTree(cdcMessage)

            logger.info("Received CDC event: processing transformed message")

            val orderData = parseOrderData(cdcEvent)
            logger.info("Processing order event: {}", orderData.orderId)
        } catch (ex: Exception) {
            logger.error("Failed to process CDC event: {}", cdcMessage, ex)
        }
    }

    private fun parseOrderData(node: JsonNode): OrderChangeData {
        return OrderChangeData(
            orderId = node.get("order_id")?.asText() ?: "",
            customerId = node.get("customer_id")?.asText() ?: "",
            quantity = node.get("quantity")?.asInt() ?: 0,
            price = node.get("price")?.asText()?.let { BigDecimal(it) } ?: BigDecimal.ZERO,
            status = node.get("status")?.asText()?.let { OrderStatus.valueOf(it) } ?: OrderStatus.PENDING,
            createdAt = parseTimestamp(node.get("created_at")?.asText()),
            updatedAt = parseTimestamp(node.get("updated_at")?.asText()),
            version = node.get("version")?.asLong() ?: 0
        )
    }

    private fun parseTimestamp(timestamp: String?): LocalDateTime? {
        return timestamp?.let {
            try {
                val epochMicros = it.toLong()
                val epochMillis = epochMicros / 1000
                java.time.Instant.ofEpochMilli(epochMillis).atZone(java.time.ZoneId.systemDefault()).toLocalDateTime()
            } catch (ex: Exception) {
                logger.warn("Failed to parse timestamp: {}", it)
                null
            }
        }
    }
}

data class OrderChangeData(
    val orderId: String,
    val customerId: String,
    val quantity: Int,
    val price: BigDecimal,
    val status: OrderStatus,
    val createdAt: LocalDateTime?,
    val updatedAt: LocalDateTime?,
    val version: Long
)