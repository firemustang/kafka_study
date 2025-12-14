package org.kafka_lecture.cdc

import jakarta.persistence.*
import java.math.BigDecimal
import java.time.LocalDateTime

@Entity
@Table(name = "orders")
class OrderEntity(
    @Id
    @Column(name = "order_id", length = 50)
    var orderId: String = "",

    @Column(name = "customer_id", length = 50, nullable = false)
    var customerId: String = "",

    @Column(name = "quantity", nullable = false)
    var quantity: Int = 0,

    @Column(name = "price", precision = 10, scale = 2, nullable = false)
    var price: BigDecimal = BigDecimal.ZERO,

    @Enumerated(EnumType.STRING)
    @Column(name = "status", length = 20, nullable = false)
    var status: OrderStatus = OrderStatus.PENDING,

    @Column(name = "created_at", nullable = false)
    var createdAt: LocalDateTime = LocalDateTime.now(),

    @Column(name = "updated_at", nullable = false)
    var updatedAt: LocalDateTime = LocalDateTime.now(),

    @Version
    @Column(name = "version")
    var version: Long = 0
) {

    @PreUpdate
    fun onPreUpdate() {
        updatedAt = LocalDateTime.now()
    }

    fun updateStatus(newStatus: OrderStatus) {
        this.status = newStatus
        this.updatedAt = LocalDateTime.now()
    }

    fun updateQuantity(newQuantity: Int) {
        require(newQuantity > 0) { "Quantity must be positive" }
        this.quantity = newQuantity
        this.updatedAt = LocalDateTime.now()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is OrderEntity) return false
        return orderId == other.orderId
    }

    override fun hashCode(): Int = orderId.hashCode()

    override fun toString(): String {
        return "OrderEntity(orderId='$orderId', customerId='$customerId', " +
                "quantity=$quantity, price=$price, status=$status)"
    }
}

enum class OrderStatus {
    PENDING,
    CONFIRMED,
    SHIPPED,
    DELIVERED,
    CANCELLED
}