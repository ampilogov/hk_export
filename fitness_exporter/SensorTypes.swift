import Foundation
import CoreLocation

// Common sensor samples without receive timestamps.
public struct HRSample: Codable {
    public let value: Int
    public let contactSupported: Bool?
    public let contactDetected: Bool?
    public let energyExpended: UInt?
    public let rrIntervals: [Double]
    public var formatted: String { "\(value) bpm" }
}

public struct HRSamples: Codable {
    public let samples: [HRSample]
}

public struct BatterySample: Codable {
    public let level: Int
    public var formatted: String { "\(level)%" }
}

// Raw ECG sample from Polar SDK
public struct ECGSample: Codable {
    // Timestamp reported by the device (in nanoseconds).
    public let timestamp: UInt64
    public let voltage: Int16
    // Convert to volts assuming microvolts input
    public var voltageVolts: Double { Double(voltage) / 1000.0 }
    public var formattedVoltage: String { String(format: "%.3f mV", voltageVolts) }
}

public struct ECGSamples: Codable {
    public let samples: [ECGSample]
}

// Raw accelerometer sample from Polar SDK
public struct AccSample: Codable {
    // Timestamp reported by the device (in nanoseconds).
    public let timestamp: UInt64
    public let x: Int16
    public let y: Int16
    public let z: Int16
    public var formattedValues: String { "x:\(x) y:\(y) z:\(z)" }
}

public struct AccSamples: Codable {
    public let samples: [AccSample]
}

// GPS location sample derived from CoreLocation's CLLocation
public struct LocationSample: Codable {
    public let latitude: Double
    public let longitude: Double
    public let altitude: Double
    public let horizontalAccuracy: Double
    public let verticalAccuracy: Double
    public init(location: CLLocation) {
        latitude = location.coordinate.latitude
        longitude = location.coordinate.longitude
        altitude = location.altitude
        horizontalAccuracy = location.horizontalAccuracy
        verticalAccuracy = location.verticalAccuracy
    }
    public var formattedCoordinate: String {
        String(format: "%.5f, %.5f", latitude, longitude)
    }
}

/// Type of captured sensor event.
enum SensorEventData {
    case hrSamples(HRSamples)
    case ecgSamples(ECGSamples)
    case accSamples(AccSamples)
    case battery(BatterySample)
    case hrvStage(HRVStage)
    case location(LocationSample)
    case custom(String)
}

/// Generic sensor event with timestamp.
struct SensorEvent {
    let timestamp: Date
    let data: SensorEventData
}
