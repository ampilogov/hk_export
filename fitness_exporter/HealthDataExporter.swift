import CoreLocation
import CryptoKit
import Foundation
import HealthKit
import Security
import zlib

func compress(data: Data) -> Data? {
    guard !data.isEmpty else { return nil }

    var stream = z_stream()
    stream.next_in = UnsafeMutablePointer<Bytef>(
        mutating: (data as NSData).bytes.bindMemory(
            to: Bytef.self, capacity: data.count))
    stream.avail_in = uint(data.count)

    let chunkSize = 16384
    var output = Data()

    // Initialize the stream for gzip compression
    deflateInit2_(
        &stream, Z_DEFAULT_COMPRESSION, Z_DEFLATED, MAX_WBITS + 16, 8,
        Z_DEFAULT_STRATEGY, ZLIB_VERSION, Int32(MemoryLayout<z_stream>.size))

    repeat {
        // Allocate a buffer for the output data
        let buffer = Data(count: chunkSize)
        stream.next_out = UnsafeMutablePointer<Bytef>(
            mutating: (buffer as NSData).bytes.bindMemory(
                to: Bytef.self, capacity: buffer.count))
        stream.avail_out = uint(buffer.count)

        // Perform the compression
        deflate(&stream, Z_FINISH)

        // Calculate the number of bytes that were actually written
        let compressedSize = buffer.count - Int(stream.avail_out)

        // Append the compressed data to the output
        output.append(buffer.prefix(compressedSize))

    } while stream.avail_out == 0

    // Clean up the stream
    deflateEnd(&stream)

    return output
}

func loadCertificate() -> SecCertificate? {
    guard let certPath = Bundle.main.path(forResource: "cert", ofType: "der")
    else {
        CustomLogger.log("Failed to find cert.der in bundle")
        return nil
    }
    guard let certData = try? Data(contentsOf: URL(fileURLWithPath: certPath))
    else {
        CustomLogger.log("Failed to load data from cert.der")
        return nil
    }
    guard
        let certificate = SecCertificateCreateWithData(nil, certData as CFData)
    else {
        CustomLogger.log("Failed to create certificate from data")
        return nil
    }
    return certificate
}

class CustomSessionDelegate: NSObject, URLSessionDelegate {
    func urlSession(
        _ session: URLSession, didReceive challenge: URLAuthenticationChallenge,
        completionHandler: @escaping (
            URLSession.AuthChallengeDisposition, URLCredential?
        ) -> Void
    ) {
        guard let serverTrust = challenge.protectionSpace.serverTrust else {
            CustomLogger.log("Failed to get server trust")
            completionHandler(.cancelAuthenticationChallenge, nil)
            return
        }

        guard let certificate = loadCertificate() else {
            CustomLogger.log("Failed to load custom certificate")
            completionHandler(.cancelAuthenticationChallenge, nil)
            return
        }

        // Get the certificate chain
        guard
            let certificates = SecTrustCopyCertificateChain(serverTrust)
                as? [SecCertificate]
        else {
            CustomLogger.log("Failed to copy certificate chain")
            completionHandler(.cancelAuthenticationChallenge, nil)
            return
        }

        for serverCertificate in certificates {
            let serverCertificateData =
                SecCertificateCopyData(serverCertificate) as Data
            let localCertificateData =
                SecCertificateCopyData(certificate) as Data

            if serverCertificateData == localCertificateData {
                let credential = URLCredential(trust: serverTrust)
                completionHandler(.useCredential, credential)
                return
            }
        }

        CustomLogger.log("Certificate not trusted")
        completionHandler(.cancelAuthenticationChallenge, nil)
    }
}

enum ExtractionError: Error {
    case unitParseError(String)
}

class Payload {
    let data: Data
    let type: String

    init(data: Data, type: String) {
        self.data = data
        self.type = type
    }

    func size() -> Int {
        return data.count
    }

    func hash() -> String {
        let hash = Data(SHA256.hash(data: data))
        let key = hash.map { String(format: "%02hhx", $0) }.joined()
        return key
    }
}

class ServerSession {
    public var server: String
    private var session: URLSession

    init(server: String, timeout: TimeInterval) {
        self.server = server

        let configuration = URLSessionConfiguration.default
        configuration.requestCachePolicy = .reloadIgnoringLocalCacheData
        configuration.urlCache = nil
        configuration.httpShouldSetCookies = true
        configuration.httpShouldUsePipelining = true
        configuration.timeoutIntervalForRequest = timeout
        configuration.timeoutIntervalForResource = timeout
        self.session = URLSession(
            configuration: configuration, delegate: CustomSessionDelegate(),
            delegateQueue: nil)
    }

    func sendPayloads(
        payloads: [[String: Any]], completion: @escaping (String?) -> Void
    ) {
        if let url = URL(string: server + "batch") {
            var request = URLRequest(url: url)
            request.httpMethod = "POST"
            request.setValue(
                "application/json", forHTTPHeaderField: "Content-Type")
            request.setValue("gzip", forHTTPHeaderField: "Content-Encoding")
            do {
                let payload = try JSONSerialization.data(withJSONObject: [
                    "payloads": payloads
                ])
                //                CustomLogger.log("Uncompressed size: \(payload.count)")
                if let compressedData = compress(data: payload) {
                    //                    CustomLogger.log("Compressed size: \(compressedData.count), \(Double(compressedData.count) / Double(payload.count))")
                    request.httpBody = compressedData
                } else {
                    return completion("Failed to compress data")
                }
            } catch {
                CustomLogger.log("Error encoding combined JSON data: \(error)")
            }

            let task = self.session.dataTask(with: request) {
                data, response, error in
                if let error = error {
                    CustomLogger.log(
                        "Client error: \(error.localizedDescription)")
                    return completion(
                        "Client error: \(error.localizedDescription)")
                }
                guard let httpResponse = response as? HTTPURLResponse,
                    (200...299).contains(httpResponse.statusCode)
                else {
                    CustomLogger.log("Server error")
                    return completion("Server error: \(response)")
                }
                CustomLogger.log("Sent")
                return completion(nil)
            }
            CustomLogger.log("Sending")
            task.resume()
        } else {
            return completion("Invalid URL: \(server)batch")
        }
    }

    func testConnection(
        completion: @escaping (String?) -> Void
    ) {
        if let url = URL(string: server + "status") {
            var request = URLRequest(url: url)
            request.httpMethod = "GET"
            let task = self.session.dataTask(with: request) {
                data, response, error in
                if let error = error {
                    return completion(
                        "Client error: \(error.localizedDescription)")
                }
                guard let httpResponse = response as? HTTPURLResponse,
                    (200...299).contains(httpResponse.statusCode)
                else {
                    return completion("Server error: \(response)")
                }
                return completion(nil)
            }
            task.resume()
        } else {
            return completion("Invalid URL: \(server)status")
        }
    }
}

class HealthDataExporter {
    static let VERSION = "v001"
    static let SENDER_EXTRA_KEY = "mnluucdsobcbkiae4a98"

    static let PAYLOAD_SEND_THRESHOLD = 100 * (1 << 20)

    private var healthStore: HKHealthStore
    private var server: String
    private var serverSession: ServerSession
    private var serverSessionQuick: ServerSession
    private var sender: String
    private var payloads: [Payload]
    private var payloadsSize = 0

    init(
        healthStore: HKHealthStore, server: String, sender: String
    ) {
        self.healthStore = healthStore
        self.server = server
        self.serverSession = ServerSession(server: server, timeout: 15)
        self.serverSessionQuick = ServerSession(server: server, timeout: 1)
        self.sender = sender

        self.payloads = []
        self.payloadsSize = 0
    }

    func export(
        sampleType: HKSampleType, from startDate: Date, to endDate: Date,
        completion: @escaping (String?) -> Void
    ) {
        self.serverSessionQuick.testConnection { result in
            if result != nil {
                return completion(result)
            }
            self.export_(
                sampleType: sampleType, from: startDate, to: endDate,
                completion: completion)
        }
    }
    private func export_(
        sampleType: HKSampleType, from startDate: Date, to endDate: Date,
        completion: @escaping (String?) -> Void
    ) {
        //        CustomLogger.log("Exporting data to \(server), \(sampleType), from \(startDate) till \(endDate)")
        CustomLogger.log(
            "Exporting data from \(startDate) till \(endDate), \(sampleType), to \(server), sender \(sender)"
        )

        if sender.isEmpty {
            return completion("Sender is empty")
        }

        let predicate = HKQuery.predicateForSamples(
            withStart: startDate, end: endDate, options: .strictStartDate)

        let query = HKSampleQuery(
            sampleType: sampleType, predicate: predicate,
            limit: HKObjectQueryNoLimit, sortDescriptors: nil
        ) { (query, samples, error) in
            guard error == nil else {
                return completion(
                    "Failed to run query: \(error?.localizedDescription ?? "WTF")"
                )
            }
            if let samples = samples {
                self.exportSamples(samples: samples) {
                    status in
                    if let status = status {
                        completion(status)
                        return
                    }
                    return self.actuallySendPayloads(completion: completion)
                }
            }
        }

        healthStore.execute(query)
    }

    private func sendPayload<T: Encodable>(
        data: T, type: String, completion: @escaping (String?) -> Void
    ) {
        var jsonData: Data? = nil
        do {
            jsonData = try JSONEncoder().encode(data)
        } catch {
            return completion("Failed to serialize data: \(error)")
        }
        self.payloads.append(Payload(data: jsonData!, type: type))
        self.payloadsSize += jsonData!.count
        if payloadsSize >= HealthDataExporter.PAYLOAD_SEND_THRESHOLD {
            return actuallySendPayloads(completion: completion)
        } else {
            return completion(nil)
        }
    }

    private func actuallySendPayloads(completion: @escaping (String?) -> Void) {
        if self.payloads.isEmpty {
            return completion(nil)
        }
        //        CustomLogger.log("Preparing to send")
        var requests: [[String: Any]] = []
        for request in self.payloads {
            do {
                if var decoded = try JSONSerialization.jsonObject(
                    with: request.data, options: []) as? [String: Any]
                {
                    // decoded["_id"] = HealthDataExporter.VERSION + "_" + request.hash()
                    decoded["_version"] = HealthDataExporter.VERSION
                    decoded["_type"] = request.type
                    decoded["_sender_sha256"] = SHA256.hash(
                        data: Data(
                            (self.sender + HealthDataExporter.SENDER_EXTRA_KEY)
                                .utf8)
                    ).compactMap { String(format: "%02x", $0) }.joined()
                    requests.append(decoded)
                }
            } catch {
                return completion("Error decoding JSON data: \(error)")
            }
        }
        self.payloads.removeAll()
        self.payloadsSize = 0

        return self.serverSession.sendPayloads(
            payloads: requests, completion: completion)
    }

    private func exportHeartBeatSeries(
        heartbeatSeries: HKHeartbeatSeriesSample,
        completion: @escaping (String?) -> Void
    ) {
        var timeSinceSeriesStartArr: [Double] = []
        var precededByGapArr: [Bool] = []

        let heartbeatSeriesQuery = HKHeartbeatSeriesQuery(
            heartbeatSeries: heartbeatSeries
        ) {
            (query, timeSinceSeriesStart, precededByGap, done, error) in
            guard error == nil else {
                return completion(
                    "Failed to run query: \(error?.localizedDescription ?? "WTF")"
                )
            }

            timeSinceSeriesStartArr.append(timeSinceSeriesStart)
            precededByGapArr.append(precededByGap)
            if done != (timeSinceSeriesStartArr.count == heartbeatSeries.count)
            {
                fatalError(
                    "HR RR query issue: \(done) \(timeSinceSeriesStartArr.count)/\(heartbeatSeries.count)"
                )
            }

            if timeSinceSeriesStartArr.count == heartbeatSeries.count {
                let cSeriesSample = self.encodeHeartbeatSeriesSample(
                    heartbeatSeriesSample: heartbeatSeries,
                    timeSinceSeriesStart: timeSinceSeriesStartArr,
                    precededByGap: precededByGapArr)
                self.sendPayload(
                    data: cSeriesSample, type: "heartbeat_series",
                    completion: completion)
            } else if timeSinceSeriesStartArr.count > heartbeatSeries.count {
                fatalError("Too many samples in a HR series")
            }
        }
        self.healthStore.execute(heartbeatSeriesQuery)
    }

    private func exportWorkoutRoute(
        workoutRoute: HKWorkoutRoute, completion: @escaping (String?) -> Void
    ) {
        var cLocations: [CCLLocation] = []

        let workoutRouteQuery = HKWorkoutRouteQuery(route: workoutRoute) {
            (query, locationsOrNil, done, error) in
            guard error == nil else {
                return completion(
                    "Failed to run query: \(error?.localizedDescription ?? "WTF")"
                )
            }

            guard let locations = locationsOrNil else {
                fatalError(
                    "*** Invalid State: This can only fail if there was an error. ***"
                )
            }

            cLocations.append(contentsOf: locations.map(self.encodeCLLocation))

            if done != (cLocations.count == workoutRoute.count) {
                fatalError(
                    "Workout route query issue: \(done) \(cLocations.count)/\(workoutRoute.count)"
                )
            }
            if cLocations.count == workoutRoute.count {
                let cLocationsArr = CCLLocations(locations: cLocations)
                self.sendPayload(
                    data: cLocationsArr, type: "workout_route",
                    completion: completion)
            } else if cLocations.count > workoutRoute.count {
                fatalError("Too many samples in workout route")
            }
        }
        self.healthStore.execute(workoutRouteQuery)
    }

    private func exportSample(
        sample: HKSample, completion: @escaping (String?) -> Void
    ) {
        if let heartbeatSeries = sample as? HKHeartbeatSeriesSample {
            return self.exportHeartBeatSeries(
                heartbeatSeries: heartbeatSeries, completion: completion)
        }

        if let workoutRoute = sample as? HKWorkoutRoute {
            return self.exportWorkoutRoute(
                workoutRoute: workoutRoute, completion: completion)
        }

        if let workout = sample as? HKWorkout {
            return self.sendPayload(
                data: self.encodeWorkout(workout: workout), type: "workout",
                completion: completion)
        }
        if let quanititySample = sample as? HKQuantitySample {
            return self.sendPayload(
                data: self.encodeQuantitySample(
                    quantitySample: quanititySample), type: "quantity_sample",
                completion: completion)
        }
        if let categorySample = sample as? HKCategorySample {
            return self.sendPayload(
                data: self.encodeCategorySample(categorySample: categorySample),
                type: "category_sample", completion: completion)
        }

        return completion(
            "Failed to cast the class: \(type(of: sample)).\n\(sample.description)"
        )
    }

    private let sampleQueue = DispatchQueue(
        label: "com.fitness_exporter.exportQueue")

    private func exportSamples(
        samples: [HKSample], completion: @escaping (String?) -> Void
    ) {
        var index = 0

        func processNextSample() {
            sampleQueue.async {
                if index == samples.count {
                    return self.actuallySendPayloads(completion: completion)
                }

                let sample = samples[index]
                self.exportSample(sample: sample) {
                    status in
                    if let status = status {
                        return completion(status)
                    }
                    index += 1
                    processNextSample()
                }
            }
        }

        processNextSample()
    }

    struct CObjectType: Codable {
        let identifier: String
    }

    private func encodeObjectType(ot: HKObjectType) -> CObjectType {
        return CObjectType(
            identifier: ot.identifier
        )
    }

    struct CSampleType: Codable {
        let superObjectType: CObjectType
        let isMinimumDurationRestricted: Bool
        let minimumAllowedDuration: TimeInterval
        let isMaximumDurationRestricted: Bool
        let maximumAllowedDuration: TimeInterval
        let allowsRecalibrationForEstimates: Bool

    }

    private func encodeSampleType(st: HKSampleType) -> CSampleType {
        return CSampleType(
            superObjectType: encodeObjectType(ot: st),
            isMinimumDurationRestricted: st.isMinimumDurationRestricted,
            minimumAllowedDuration: st.minimumAllowedDuration,
            isMaximumDurationRestricted: st.isMaximumDurationRestricted,
            maximumAllowedDuration: st.maximumAllowedDuration,
            allowsRecalibrationForEstimates: st.allowsRecalibrationForEstimates
        )
    }

    struct CQuantityType: Codable {
        let superSampleType: CSampleType
        let aggregationStyle: Int
    }

    private func encodeQuantityType(quantityType: HKQuantityType)
        -> CQuantityType
    {
        return CQuantityType(
            superSampleType: encodeSampleType(st: quantityType),
            aggregationStyle: quantityType.aggregationStyle.rawValue)
    }

    struct CDevice: Codable {
        let udiDeviceIdentifier: String?
        let firmwareVersion: String?
        let hardwareVersion: String?
        let localIdentifier: String?
        let manufacturer: String?
        let model: String?
        let name: String?
        let softwareVersion: String?
    }

    private func encodeDevice(device: HKDevice) -> CDevice {
        return CDevice(
            udiDeviceIdentifier: device.udiDeviceIdentifier,
            firmwareVersion: device.firmwareVersion,
            hardwareVersion: device.hardwareVersion,
            localIdentifier: device.localIdentifier,
            manufacturer: device.manufacturer,
            model: device.model,
            name: device.name,
            softwareVersion: device.softwareVersion
        )
    }

    struct COperationSystemVersion: Codable {
        let majorVersion: Int
        let minorVersion: Int
        let patchVersion: Int
    }

    private func encodeOperationSystemVersion(osv: OperatingSystemVersion)
        -> COperationSystemVersion
    {
        return COperationSystemVersion(
            majorVersion: osv.majorVersion,
            minorVersion: osv.minorVersion,
            patchVersion: osv.patchVersion
        )
    }

    struct CSource: Codable {
        let bundleIdentifier: String
        let name: String
    }

    private func encodeSource(source: HKSource) -> CSource {
        return CSource(
            bundleIdentifier: source.bundleIdentifier, name: source.name)
    }

    struct CSourceRevision: Codable {
        let source: CSource
        let version: String?
        let operatingSystemVersion: COperationSystemVersion
        let productType: String?
    }

    private func encodeSourceRevision(sr: HKSourceRevision) -> CSourceRevision {
        return CSourceRevision(
            source: encodeSource(source: sr.source),
            version: sr.version,
            operatingSystemVersion: encodeOperationSystemVersion(
                osv: sr.operatingSystemVersion),
            productType: sr.productType
        )
    }

    struct CObject: Codable {
        let uuid: UUID
        let metadata: [String: String]?
        let device: CDevice?
        let sourceRevision: CSourceRevision
    }

    private func encodeObject(object: HKObject) -> CObject {
        return CObject(
            uuid: object.uuid,
            metadata: object.metadata?.mapValues({ "\($0)" }),
            device: object.device.map { encodeDevice(device: $0) },
            sourceRevision: encodeSourceRevision(sr: object.sourceRevision)
        )
    }

    struct CSample: Codable {
        let superObject: CObject
        let startDate: Date
        let endDate: Date
        let hasUndeterminedDuration: Bool
        let sampleType: CSampleType
    }

    private func encodeSample(sample: HKSample) -> CSample {
        return CSample(
            superObject: encodeObject(object: sample),
            startDate: sample.startDate,
            endDate: sample.endDate,
            hasUndeterminedDuration: sample.hasUndeterminedDuration,
            sampleType: encodeSampleType(st: sample.sampleType)
        )
    }

    struct CQuantity: Codable {
        let unit: String
        let doubleValue: Double
    }

    private func encodeQuantity(quantity: HKQuantity) -> CQuantity {
        for unit in HealthDataExporter.UNITS {
            if quantity.is(compatibleWith: unit) {
                return CQuantity(
                    unit: unit.unitString,
                    doubleValue: quantity.doubleValue(for: unit))
            }
        }
        CustomLogger.log("Can't find a unit for: \(quantity.description)")
        return CQuantity(
            unit: "unknown: \(quantity.description)", doubleValue: Double.nan)
    }

    struct CStatistics: Codable {
        let startDate: Date
        let endDate: Date
        let quantityType: CQuantityType
        let sources: [CSource]?
        let sourceAverageQuantity: [String: CQuantity?]?
        let averageQuantity: CQuantity?
        let sourceMaximumQuantity: [String: CQuantity?]?
        let maximumQuantity: CQuantity?
        let sourceMinimumQuantity: [String: CQuantity?]?
        let minimumQuantity: CQuantity?
        let sourceSumQuantity: [String: CQuantity?]?
        let sumQuantity: CQuantity?
        let sourceDuration: [String: CQuantity?]?
        let duration: CQuantity?
        let sourceMostRecentQuantity: [String: CQuantity?]?
        let mostRecentQuantity: CQuantity?
        let sourceMostRecentQuantityDateInterval: [String: DateInterval?]?
        let mostRecentQuantityDateInterval: DateInterval?
    }

    private func encodeStatistic(statistic: HKStatistics) -> CStatistics {
        let sources = statistic.sources?.compactMap {
            source -> (key: String, value: HKSource) in
            return (key: source.name, source)
        }.reduce(into: [String: HKSource]()) {
            dict, tuple in
            dict[tuple.key] = tuple.value
        }

        return CStatistics(
            startDate: statistic.startDate,
            endDate: statistic.endDate,
            quantityType: encodeQuantityType(
                quantityType: statistic.quantityType),
            sources: statistic.sources?.map { encodeSource(source: $0) },
            sourceAverageQuantity: sources?.mapValues {
                statistic.averageQuantity(for: $0).map {
                    encodeQuantity(quantity: $0)
                }
            },
            averageQuantity: statistic.averageQuantity().map {
                encodeQuantity(quantity: $0)
            },
            sourceMaximumQuantity: sources?.mapValues {
                statistic.maximumQuantity(for: $0).map {
                    encodeQuantity(quantity: $0)
                }
            },
            maximumQuantity: statistic.maximumQuantity().map {
                encodeQuantity(quantity: $0)
            },
            sourceMinimumQuantity: sources?.mapValues {
                statistic.minimumQuantity(for: $0).map {
                    encodeQuantity(quantity: $0)
                }
            },
            minimumQuantity: statistic.minimumQuantity().map {
                encodeQuantity(quantity: $0)
            },
            sourceSumQuantity: sources?.mapValues {
                statistic.sumQuantity(for: $0).map {
                    encodeQuantity(quantity: $0)
                }
            },
            sumQuantity: statistic.sumQuantity().map {
                encodeQuantity(quantity: $0)
            },
            sourceDuration: sources?.mapValues {
                statistic.duration(for: $0).map { encodeQuantity(quantity: $0) }
            },
            duration: statistic.duration().map { encodeQuantity(quantity: $0) },
            sourceMostRecentQuantity: sources?.mapValues {
                statistic.mostRecentQuantity(for: $0).map {
                    encodeQuantity(quantity: $0)
                }
            },
            mostRecentQuantity: statistic.mostRecentQuantity().map {
                encodeQuantity(quantity: $0)
            },
            sourceMostRecentQuantityDateInterval: sources?.mapValues {
                statistic.mostRecentQuantityDateInterval(for: $0)
            },
            mostRecentQuantityDateInterval:
                statistic.mostRecentQuantityDateInterval()
        )
    }

    struct CWorkoutAllStatisticEntry: Codable {
        let quantityType: CQuantityType
        let statistic: CStatistics
    }

    struct CWorkoutConfiguration: Codable {
        let activityType: UInt
        let locationType: Int
        let swimmingLocationType: Int
        let lapLength: CQuantity?

    }

    private func encodeWorkoutConfiguration(
        configuration: HKWorkoutConfiguration
    ) -> CWorkoutConfiguration {
        return CWorkoutConfiguration(
            activityType: configuration.activityType.rawValue,
            locationType: configuration.locationType.rawValue,
            swimmingLocationType: configuration.swimmingLocationType.rawValue,
            lapLength: configuration.lapLength.map {
                encodeQuantity(quantity: $0)
            })
    }

    struct CWorkoutEvent: Codable {
        let dateInterval: DateInterval
        let type: Int
        let metadata: [String: String]?
    }

    private func encodeWorkoutEvent(event: HKWorkoutEvent) -> CWorkoutEvent {
        return CWorkoutEvent(
            dateInterval: event.dateInterval,
            type: event.type.rawValue,
            metadata: event.metadata?.mapValues({ "\($0)" })
        )
    }

    struct CWorkoutActivity: Codable {
        let uuid: UUID
        let startDate: Date
        let endDate: Date?
        let duration: TimeInterval
        let allStatistics: [CWorkoutAllStatisticEntry]
        let metadata: [String: String]?
        let workoutConfiguration: CWorkoutConfiguration
        let workoutEvents: [CWorkoutEvent]
    }

    private func encodeWorkoutActivity(activity: HKWorkoutActivity)
        -> CWorkoutActivity
    {
        return CWorkoutActivity(
            uuid: activity.uuid,
            startDate: activity.startDate,
            endDate: activity.endDate,
            duration: activity.duration,
            allStatistics: activity.allStatistics.map {
                CWorkoutAllStatisticEntry(
                    quantityType: encodeQuantityType(quantityType: $0),
                    statistic: encodeStatistic(statistic: $1)
                )
            },
            metadata: activity.metadata?.mapValues({ "\($0)" }),
            workoutConfiguration: encodeWorkoutConfiguration(
                configuration: activity.workoutConfiguration),
            workoutEvents: activity.workoutEvents.map {
                encodeWorkoutEvent(event: $0)
            })
    }

    struct CWorkout: Codable {
        let superSample: CSample
        let duration: TimeInterval
        let workoutActivityType: UInt
        let workoutActivities: [CWorkoutActivity]
        let workoutEvents: [CWorkoutEvent]?
        let allStatistics: [CWorkoutAllStatisticEntry]
    }

    private func encodeWorkout(workout: HKWorkout) -> CWorkout {
        return CWorkout(
            superSample: encodeSample(sample: workout),
            duration: workout.duration,
            workoutActivityType: workout.workoutActivityType.rawValue,
            workoutActivities: workout.workoutActivities.map {
                encodeWorkoutActivity(activity: $0)
            },
            workoutEvents: workout.workoutEvents?.map {
                encodeWorkoutEvent(event: $0)
            },
            allStatistics: workout.allStatistics.map {
                CWorkoutAllStatisticEntry(
                    quantityType: encodeQuantityType(quantityType: $0),
                    statistic: encodeStatistic(statistic: $1)
                )
            }
        )
    }

    struct CQuantitySample: Codable {
        let superSample: CSample
        let quantity: CQuantity
        let count: Int
        let quantityType: CQuantityType
    }

    private func encodeQuantitySample(quantitySample: HKQuantitySample)
        -> CQuantitySample
    {
        return CQuantitySample(
            superSample: encodeSample(sample: quantitySample),
            quantity: encodeQuantity(quantity: quantitySample.quantity),
            count: quantitySample.count,
            quantityType: encodeQuantityType(
                quantityType: quantitySample.quantityType))
    }

    struct CSeriesSample: Codable {
        let superSample: CSample
        let count: Int
    }

    private func encodeSeriesSample(seriesSample: HKSeriesSample)
        -> CSeriesSample
    {
        return CSeriesSample(
            superSample: encodeSample(sample: seriesSample),
            count: seriesSample.count)
    }

    struct CHeartbeatSeriesSample: Codable {
        let seriesSample: CSeriesSample
        let timeSinceSeriesStart: [Double]
        let precededByGap: [Bool]
    }

    private func encodeHeartbeatSeriesSample(
        heartbeatSeriesSample: HKHeartbeatSeriesSample,
        timeSinceSeriesStart: [Double],
        precededByGap: [Bool]
    ) -> CHeartbeatSeriesSample {
        return CHeartbeatSeriesSample(
            seriesSample: encodeSeriesSample(
                seriesSample: heartbeatSeriesSample),
            timeSinceSeriesStart: timeSinceSeriesStart,
            precededByGap: precededByGap)
    }

    struct CCLLocationSourceInformation: Codable {
        let isProducedByAccessory: Bool
        let isSimulatedBySoftware: Bool
    }

    struct CCLLocation: Codable {
        let latitude: Double
        let longitude: Double
        let altitude: Double
        let ellipsoidalAltitude: Double
        let floor: Int?
        let timestamp: Date
        let sourceInformation: CCLLocationSourceInformation?
        let horizontalAccuracy: Double
        let verticalAccuracy: Double
        let speed: Double
        let speedAccuracy: Double
        let course: Double
        let courseAccuracy: Double
    }

    struct CCLLocations: Codable {
        let locations: [CCLLocation]
    }

    private func encodeCLLocation(location: CLLocation) -> CCLLocation {
        return CCLLocation(
            latitude: location.coordinate.latitude,
            longitude: location.coordinate.longitude,
            altitude: location.altitude,
            ellipsoidalAltitude: location.ellipsoidalAltitude,
            floor: location.floor?.level,
            timestamp: location.timestamp,
            sourceInformation: location.sourceInformation == nil
                ? nil
                : CCLLocationSourceInformation(
                    isProducedByAccessory: location.sourceInformation!
                        .isProducedByAccessory,
                    isSimulatedBySoftware: location.sourceInformation!
                        .isSimulatedBySoftware),
            horizontalAccuracy: location.horizontalAccuracy,
            verticalAccuracy: location.verticalAccuracy,
            speed: location.speed,
            speedAccuracy: location.speedAccuracy,
            course: location.course,
            courseAccuracy: location.courseAccuracy)
    }

    struct CCategorySample: Codable {
        let superSample: CSample
        let categoryType: CSampleType
        let value: Int
    }

    private func encodeCategorySample(categorySample: HKCategorySample)
        -> CCategorySample
    {
        return CCategorySample(
            superSample: encodeSample(sample: categorySample),
            categoryType: encodeSampleType(st: categorySample.categoryType),
            value: categorySample.value)
    }

    static let UNITS = [
        HKUnit.gram(),
        HKUnit.meter(),
        HKUnit.liter(),
        HKUnit.second(),
        HKUnit.largeCalorie(),
        HKUnit.watt(),
        HKUnit.degreeFahrenheit(),
        HKUnit.decibelHearingLevel(),
        HKUnit.init(from: "count/s"),
        HKUnit.diopter(),
        HKUnit.degreeAngle(),
        HKUnit.count(),
        HKUnit.percent(),
        HKUnit.meter().unitDivided(by: HKUnit.second()),
        HKUnit.decibelAWeightedSoundPressureLevel(),
        HKUnit.init(from: "mL/min·kg"),
        HKUnit.init(from: "kcal/hr·kg"),
        //            HKUnit.siemen(),
        //            HKUnit.volt(),
        //            HKUnit.internationalUnit(),
        //            HKUnit.pascal(),
    ]

    static let QUANTITY_TYPES: [HKQuantityTypeIdentifier] = [
        .stepCount,
        .distanceWalkingRunning,
        .runningGroundContactTime,
        .runningPower,
        .runningSpeed,
        .runningStrideLength,
        .runningVerticalOscillation,
        .distanceCycling,
        .pushCount,
        .distanceWheelchair,
        .swimmingStrokeCount,
        .distanceSwimming,
        .distanceDownhillSnowSports,
        .basalEnergyBurned,
        .activeEnergyBurned,
        .flightsClimbed,
        .nikeFuel,
        .appleExerciseTime,
        .appleMoveTime,
        .appleStandTime,
        .vo2Max,
        .height,
        .bodyMass,
        .bodyMassIndex,
        .leanBodyMass,
        .bodyFatPercentage,
        .waistCircumference,
        .appleSleepingWristTemperature,
        .basalBodyTemperature,
        .environmentalAudioExposure,
        .headphoneAudioExposure,
        .heartRate,
        .restingHeartRate,
        .walkingHeartRateAverage,
        .heartRateVariabilitySDNN,
        .heartRateRecoveryOneMinute,
        .atrialFibrillationBurden,
        .oxygenSaturation,
        .bodyTemperature,
        .bloodPressureDiastolic,
        .bloodPressureSystolic,
        .respiratoryRate,
        .bloodGlucose,
        .electrodermalActivity,
        .forcedExpiratoryVolume1,
        .forcedVitalCapacity,
        .inhalerUsage,
        .insulinDelivery,
        .numberOfTimesFallen,
        .peakExpiratoryFlowRate,
        .peripheralPerfusionIndex,
        .appleSleepingWristTemperature,
        .dietaryBiotin,
        .dietaryCaffeine,
        .dietaryCalcium,
        .dietaryCarbohydrates,
        .dietaryChloride,
        .dietaryCholesterol,
        .dietaryChromium,
        .dietaryCopper,
        .dietaryEnergyConsumed,
        .dietaryFatMonounsaturated,
        .dietaryFatPolyunsaturated,
        .dietaryFatSaturated,
        .dietaryFatTotal,
        .dietaryFiber,
        .dietaryFolate,
        .dietaryIodine,
        .dietaryIron,
        .dietaryMagnesium,
        .dietaryManganese,
        .dietaryMolybdenum,
        .dietaryNiacin,
        .dietaryPantothenicAcid,
        .dietaryPhosphorus,
        .dietaryPotassium,
        .dietaryProtein,
        .dietaryRiboflavin,
        .dietarySelenium,
        .dietarySodium,
        .dietarySugar,
        .dietaryThiamin,
        .dietaryVitaminA,
        .dietaryVitaminB12,
        .dietaryVitaminB6,
        .dietaryVitaminC,
        .dietaryVitaminD,
        .dietaryVitaminE,
        .dietaryVitaminK,
        .dietaryWater,
        .dietaryZinc,
        .bloodAlcoholContent,
        .numberOfAlcoholicBeverages,
        .appleWalkingSteadiness,
        .sixMinuteWalkTestDistance,
        .walkingSpeed,
        .walkingStepLength,
        .walkingAsymmetryPercentage,
        .walkingDoubleSupportPercentage,
        .stairAscentSpeed,
        .stairDescentSpeed,
        .uvExposure,
        .underwaterDepth,
        .waterTemperature,
        .cyclingCadence,
        .cyclingFunctionalThresholdPower,
        .cyclingPower,
        .cyclingSpeed,
        .environmentalSoundReduction,
        .physicalEffort,
        .timeInDaylight,
    ]
}
