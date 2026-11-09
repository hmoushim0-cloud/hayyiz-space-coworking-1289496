package sa.factory.hayyizspacecoworki.data.repository

import sa.factory.hayyizspacecoworki.domain.repository.HayyizSpaceRepository
import sa.factory.hayyizspacecoworki.hardware.HardwareSensorDriver
import sa.factory.hayyizspacecoworki.model.HayyizSpaceItem
import sa.factory.hayyizspacecoworki.model.HayyizSpaceOrder
import sa.factory.hayyizspacecoworki.model.HayyizSpaceTelemetry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch

class HayyizSpaceRepositoryImpl(
    private val sensorDriver: HardwareSensorDriver
) : HayyizSpaceRepository {

    private val _catalog = MutableStateFlow<List<HayyizSpaceItem>>(listOf(
        HayyizSpaceItem("1", "خدمة إدارة المهام", "تنظيم الأولويات اليومية مع تتبع الحالة في الوقت الفعلي", 150.0, "معتمد", "المهام الإدارية"),
        HayyizSpaceItem("2", "وصول RFID فوري", "دمج بوابة الدخول 13.56 MHz مع مصادقة TOTP متعددة العوامل", 280.0, "فوري", "إدارة الوصول"),
        HayyizSpaceItem("3", "تصديق TOTP", "توليد رموز مرور مؤقتة متوافقة مع RFC 6238", 95.0, "جديد", "تنظيم البيانات")
    ))
    override val catalogFlow: Flow<List<HayyizSpaceItem>> = _catalog.asStateFlow()

    private val _cart = MutableStateFlow<List<HayyizSpaceItem>>(emptyList())
    override val activeCart: StateFlow<List<HayyizSpaceItem>> = _cart.asStateFlow()

    override val telemetryFlow: Flow<HayyizSpaceTelemetry> = sensorDriver.observePeripheralStream()
        .map { frame ->
            HayyizSpaceTelemetry(
                peripheralName = frame.peripheralName,
                status = frame.connectionState,
                liveReading = frame.livePayload,
                batteryPct = 96,
                signalDbm = -54
            )
        }

    override fun addToCart(item: HayyizSpaceItem) {
        val current = _cart.value.toMutableList()
        current.add(item)
        _cart.value = current
    }

    override fun removeFromCart(item: HayyizSpaceItem) {
        val current = _cart.value.toMutableList()
        current.remove(item)
        _cart.value = current
    }

    override fun clearCart() {
        _cart.value = emptyList()
    }

    override suspend fun checkout(paymentMethod: String): HayyizSpaceOrder {
        val currentItems = _cart.value.toList()
        val subtotal = currentItems.sumOf { it.priceSar }
        val vat = subtotal * 0.15
        val total = subtotal + vat
        val order = HayyizSpaceOrder(
            orderId = "ORD-${System.currentTimeMillis() % 100000}",
            items = currentItems,
            subtotalSar = subtotal,
            vatSar = vat,
            totalSar = total,
            paymentMethod = paymentMethod
        )
        clearCart()
        return order
    }

    override suspend fun refreshCatalog() {
        // Offline-first sync logic
    }
}
