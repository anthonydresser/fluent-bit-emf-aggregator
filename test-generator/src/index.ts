import { createMetricsLogger, Unit } from 'aws-embedded-metrics';

// Utility to generate random number within a range
function randomInRange(min: number, max: number): number {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

// Simulate different types of events
type EventType = 'order' | 'payment' | 'inventory' | 'user_session';
const eventTypes: EventType[] = ['order', 'payment', 'inventory', 'user_session'];

// Generate realistic user session data
function generateSessionData() {
    return {
        userId: `user_${randomInRange(1, 1000000)}`,
        deviceType: ['mobile', 'desktop', 'tablet'][randomInRange(0, 2)],
        browser: ['chrome', 'firefox', 'safari', 'edge'][randomInRange(0, 3)],
        location: ['us-east-1', 'us-west-2', 'eu-west-1', 'ap-southeast-1'][randomInRange(0, 3)]
    };
}

// Generate realistic order data
function generateOrderData() {
    return {
        orderId: `ord_${Date.now()}_${randomInRange(1, 1000)}`,
        items: randomInRange(1, 10),
        total: parseFloat((randomInRange(1000, 50000) / 100).toFixed(2)),
        currency: 'USD'
    };
}

// Generate realistic payment data
function generatePaymentData() {
    return {
        paymentId: `pay_${Date.now()}_${randomInRange(1, 1000)}`,
        method: ['credit_card', 'debit_card', 'paypal', 'crypto'][randomInRange(0, 3)],
        status: ['success', 'success', 'success', 'failed'][randomInRange(0, 3)], // 75% success rate
        processingTime: randomInRange(100, 2000)
    };
}

// Generate realistic inventory data
function generateInventoryData() {
    return {
        productId: `prod_${randomInRange(1, 10000)}`,
        quantity: randomInRange(0, 1000),
        warehouse: `wh_${randomInRange(1, 5)}`,
        reorderPoint: randomInRange(50, 200)
    };
}

const emitMetrics = async () => {
    const metrics = createMetricsLogger()
    const eventType = eventTypes[randomInRange(0, eventTypes.length - 1)];
    const timestamp = Date.now();

    // Set common dimensions
    const baseDimensions = ({
        Service: 'EcommerceApp',
        Environment: 'Production',
        Region: 'us-west-2'
    });

    metrics.setNamespace('EcommerceMetrics');
    metrics.setTimestamp(new Date(timestamp));

    switch (eventType) {
        case 'user_session': {
            const sessionData = generateSessionData();
            metrics.setDimensions({
                ...baseDimensions,
                DeviceType: sessionData.deviceType,
                Browser: sessionData.browser
            });

            metrics.putMetric('SessionDuration', randomInRange(10, 3600), Unit.Seconds);
            metrics.putMetric('PageViews', randomInRange(1, 50), Unit.Count);
            metrics.putMetric('BounceRate', randomInRange(20, 80), Unit.Percent);
            metrics.putMetric('LoadTime', randomInRange(100, 2000), Unit.Milliseconds);
            metrics.setProperty('sessionData', sessionData);
            break;
        }

        case 'order': {
            const orderData = generateOrderData();
            metrics.putMetric('OrderValue', orderData.total, Unit.None);
            metrics.putMetric('ItemsPerOrder', orderData.items, Unit.Count);
            metrics.putMetric('OrderProcessingTime', randomInRange(500, 3000), Unit.Milliseconds);
            metrics.putMetric('CartAbandonmentRate', randomInRange(20, 40), Unit.Percent);
            metrics.setProperty('orderData', orderData);
            break;
        }

        case 'payment': {
            const paymentData = generatePaymentData();
            metrics.setDimensions({
                ...baseDimensions,
                PaymentMethod: paymentData.method,
                PaymentStatus: paymentData.status
            });

            metrics.putMetric('PaymentProcessingTime', paymentData.processingTime, Unit.Milliseconds);
            metrics.putMetric('PaymentSuccess', paymentData.status === 'success' ? 1 : 0, Unit.Count);
            metrics.putMetric('PaymentFailure', paymentData.status === 'failed' ? 1 : 0, Unit.Count);
            metrics.putMetric('TransactionValue', randomInRange(1000, 50000) / 100, Unit.None);
            metrics.setProperty('paymentData', paymentData);
            break;
        }

        case 'inventory': {
            const inventoryData = generateInventoryData();
            metrics.setDimensions({
                ...baseDimensions,
                Warehouse: inventoryData.warehouse
            });

            metrics.putMetric('StockLevel', inventoryData.quantity, Unit.Count);
            metrics.putMetric('StockValue', inventoryData.quantity * randomInRange(10, 100), Unit.None);
            metrics.putMetric('OutOfStock', inventoryData.quantity === 0 ? 1 : 0, Unit.Count);
            metrics.putMetric('LowStock', inventoryData.quantity < inventoryData.reorderPoint ? 1 : 0, Unit.Count);
            metrics.setProperty('inventoryData', inventoryData);
            break;
        }
    }

    // Add some system metrics
    metrics.putMetric('CPUUtilization', randomInRange(20, 95), Unit.Percent);
    metrics.putMetric('MemoryUtilization', randomInRange(30, 85), Unit.Percent);
    metrics.putMetric('LatencyP95', randomInRange(50, 500), Unit.Milliseconds);
    metrics.putMetric('ErrorRate', randomInRange(0, 5), Unit.Percent);
    await metrics.flush()
};

// Usage remains the same
async function startEmitting(
    batchSize: number = 1000,
    intervalMs: number = 1000,
    maxRunTimeMs?: number
) {
    console.log(`Starting EMF emission with batch size: ${batchSize}, interval: ${intervalMs}ms`);
    
    const startTime = Date.now();
    let totalEmitted = 0;
    
    const interval = setInterval(async () => {
        const currentTime = Date.now();
        if (maxRunTimeMs && currentTime - startTime >= maxRunTimeMs) {
            clearInterval(interval);
            console.log(`Finished emitting. Total emitted: ${totalEmitted} batches`);
            return;
        }

        try {
            const promises = Array(batchSize).fill(0).map(() => emitMetrics());
            await Promise.all(promises);
            totalEmitted += batchSize;
            
            console.log(`Emitted batch of ${batchSize} metrics. Total: ${totalEmitted}`);
        } catch (error) {
            console.error('Error emitting metrics:', error);
        }
    }, intervalMs);
}

// Main execution
async function main() {
    const batchSize = parseInt(process.env.BATCH_SIZE || '1000', 10);
    const intervalMs = parseInt(process.env.INTERVAL_MS || '1000', 10);
    const maxRunTimeMs = parseInt(process.env.MAX_RUNTIME_MS || '60000', 10);

    try {
        await startEmitting(batchSize, intervalMs, maxRunTimeMs);
    } catch (error) {
        console.error('Error in main execution:', error);
    }
}

// Handle graceful shutdown
process.on('SIGINT', () => {
    console.log('Received SIGINT. Shutting down...');
    process.exit(0);
});

process.on('SIGTERM', () => {
    console.log('Received SIGTERM. Shutting down...');
    process.exit(0);
});

// Start the emission
main().catch(console.error);
