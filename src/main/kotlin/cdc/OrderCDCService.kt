package org.kafka_lecture.cdc


import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Repository
interface OrderRepository : JpaRepository<OrderEntity, String>

@Service
@Transactional
class OrderCdcService(
    private val orderRepository: OrderRepository
) {

    private val logger = org.slf4j.LoggerFactory.getLogger(OrderCdcService::class.java)

    fun createOrder(
        orderId: String,
        customerId: String,
        quantity: Int,
        price: java.math.BigDecimal
    ): OrderEntity {
        if (orderRepository.existsById(orderId)) {
            throw IllegalArgumentException("Order with ID $orderId already exists")
        }

        val order = OrderEntity(
            orderId = orderId,
            customerId = customerId,
            quantity = quantity,
            price = price,
            status = OrderStatus.PENDING
        )

        val savedOrder = orderRepository.save(order)
        logger.info("Created order: {} (Debezium will automatically capture this DB change)", orderId)

        return savedOrder
    }

}