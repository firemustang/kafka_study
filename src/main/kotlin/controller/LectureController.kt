package org.kafka_lecture.controller

import org.kafka_lecture.avro.AvroOrderEventProducer
import org.kafka_lecture.basic.OrderEventPublisher
import org.kafka_lecture.cdc.OrderCdcService
import org.kafka_lecture.model.CreateOrderRequest
import org.kafka_lecture.model.OrderEvent
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.math.BigDecimal
import java.util.UUID


@RestController
@RequestMapping("/api/lecture")
class LectureController(
    private val orderEventPublisher: OrderEventPublisher,
    private val avroEventPublisher: AvroOrderEventProducer,
    private val cdcService: OrderCdcService,
) {

    @PostMapping
    fun createOrder(@RequestBody request : CreateOrderRequest) : ResponseEntity<String> {
        val orderEvent = OrderEvent(
            orderId = UUID.randomUUID().toString(),
            customerId = request.customerId,
            quantity = request.quantity,
            price = request.price,
        )

        orderEventPublisher.publishOrderEvent(orderEvent)

        return ResponseEntity.ok("Order created")
    }

    @PostMapping("/avro/publish")
    fun createOrder(
        @RequestParam(defaultValue = "CUST-123") customerId: String,
        @RequestParam(defaultValue = "5") quantity: Int,
        @RequestParam(defaultValue = "99.99") price: BigDecimal
    ): Map<String, Any> {

        val orderId = UUID.randomUUID().toString()

        avroEventPublisher.publishOrderEvent(
            orderId = orderId,
            customerId = customerId,
            quantity = quantity,
            price = price
        )

        return mapOf(
            "success" to true,
            "orderId" to orderId,
            "message" to "Avro order event published successfully"
        )
    }

    @PostMapping("/cdc/create")
    fun createOrderForCdc(@RequestBody request: CreateOrderRequest): ResponseEntity<String> {
        val orderId = UUID.randomUUID().toString()

        val savedOrder = cdcService.createOrder(
            orderId = orderId,
            customerId = request.customerId,
            quantity = request.quantity,
            price = request.price
        )

        return ResponseEntity.ok("Order created in DB (CDC will trigger): ${savedOrder.orderId}")
    }

}