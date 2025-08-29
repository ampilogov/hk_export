import CoreBluetooth

/// Commonly used Bluetooth service and characteristic UUIDs.
enum BluetoothUUID {
    static let heartRateService = CBUUID(string: "180D")
    static let batteryService = CBUUID(string: "180F")
    static let heartRateMeasurement = CBUUID(string: "2A37")
    static let batteryLevel = CBUUID(string: "2A19")
}
