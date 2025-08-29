import CoreLocation
import CryptoKit
import Foundation
import HealthKit
import Security
import zlib

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

class HealthDataExporter {
    static let VERSION = "v004"
    static let SENDER_EXTRA_KEY = "mnluucdsobcbkiae4a98"

    static let PAYLOAD_SEND_THRESHOLD = 10 * (1 << 20)

    private var server: String
    private var serverSession: ServerSession
    private var sender: String
    private var payloads: [Payload]
    private var payloadsSize = 0

    init(server: String, sender: String) {
        self.server = server
        self.serverSession = ServerSession.getSession(server: server)
        self.sender = sender

        self.payloads = []
        self.payloadsSize = 0
    }

    func export(
        sampleType: HKSampleType, from startDate: Date, to endDate: Date,
        completion: @escaping (String?) -> Void
    ) {
        self.export_(
            sampleType: sampleType, from: startDate, to: endDate,
            completion: completion)
    }

    private func export_(
        sampleType: HKSampleType, from startDate: Date, to endDate: Date,
        completion: @escaping (String?) -> Void
    ) {
        //        CustomLogger.log("Exporting data to \(server), \(sampleType), from \(startDate) till \(endDate)")
        CustomLogger.log(
            "[HDE][Info] Exporting data from \(startDate) till \(endDate), \(sampleType), to \(server), sender \(sender)"
        )

        if sender.isEmpty {
            return completion("Sender is empty")
        }

        guard HKHealthStore.isHealthDataAvailable() else {
            return completion("Health store is not available")
        }
        let healthStore = HKHealthStore()

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
                self.exportSamples(healthStore: healthStore, samples: samples) {
                    status in
                    if let status = status {
                        completion(status)
                        return
                    }
                    return self.actuallySendPayloadsPList(
                        completion: completion)
                }
            }
        }

        healthStore.execute(query)
    }

    private func exportSample(
        healthStore: HKHealthStore, sample: HKSample,
        completion: @escaping (String?) -> Void
    ) {
        do {
            if let heartbeatSeries = sample as? HKHeartbeatSeriesSample {
                return self.exportHeartBeatSeries(
                    healthStore: healthStore, heartbeatSeries: heartbeatSeries,
                    completion: completion)
            }

            if let workoutRoute = sample as? HKWorkoutRoute {
                return self.exportWorkoutRoute(
                    healthStore: healthStore, workoutRoute: workoutRoute,
                    completion: completion)
            }

            if let workout = sample as? HKWorkout {
                return self.sendPayload(
                    data: try self.encodeWorkout(workout: workout),
                    type: "workout",
                    completion: completion)
            }

            if let quanititySample = sample as? HKQuantitySample {
                let series = extractSeries(
                    quanititySample: quanititySample, healthStore: healthStore)
                return self.sendPayload(
                    data: try self.encodeQuantitySample(
                        quantitySample: quanititySample, series: series),
                    type: "quantity_sample",
                    completion: completion)
            }
            if let categorySample = sample as? HKCategorySample {
                return self.sendPayload(
                    data: self.encodeCategorySample(
                        categorySample: categorySample),
                    type: "category_sample", completion: completion)
            }
            if let clinicalRecord = sample as? HKClinicalRecord {
                return self.sendPayload(
                    data: self.encodeClinicalRecord(
                        clinicalRecord: clinicalRecord),
                    type: "clinical_record", completion: completion)
            }
            if let stateOfMind = sample as? HKStateOfMind {
                return self.sendPayload(
                    data: self.encodeStateOfMind(stateOfMind: stateOfMind),
                    type: "state_of_mind", completion: completion)
            }

            return completion(
                "Failed to cast the class: \(type(of: sample)).\n\(sample.description)"
            )
        } catch {
            return completion(
                "Encountered error while exporting sample: \(error.localizedDescription)"
            )
        }
    }

    private func sendPayloadJson<T: Encodable>(
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
            return actuallySendPayloadsJson(completion: completion)
        } else {
            return completion(nil)
        }
    }

    private func sendPayloadPList<T: Encodable>(
        data: T, type: String, completion: @escaping (String?) -> Void
    ) {
        do {
            let encoder = PropertyListEncoder()
            encoder.outputFormat = .binary
            let plistData = try encoder.encode(data)
            let payload = Payload(data: plistData, type: type)
            self.payloads.append(payload)
            self.payloadsSize += plistData.count
        } catch {
            return completion(
                "Failed to serialize data: \(error.localizedDescription)")
        }
        if payloadsSize >= HealthDataExporter.PAYLOAD_SEND_THRESHOLD {
            return actuallySendPayloadsPList(completion: completion)
        } else {
            return completion(nil)
        }
    }

    private func actuallySendPayloadsJson(
        completion: @escaping (String?) -> Void
    ) {
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

        return self.serverSession.sendPayloadsPList(
            payloads: requests, completion: completion)
    }

    private func actuallySendPayloadsPList(
        completion: @escaping (String?) -> Void
    ) {
        if self.payloads.isEmpty {
            return completion(nil)
        }
        //        CustomLogger.log("Preparing to send")
        var requests: [[String: Any]] = []
        for request in self.payloads {
            let payload =
                [
                    "version": HealthDataExporter.VERSION,
                    "data": request.data,
                    "type": request.type,
                    "sender_sha256": SHA256.hash(
                        data: Data(
                            (self.sender
                                + HealthDataExporter.SENDER_EXTRA_KEY)
                                .utf8)
                    ).compactMap { String(format: "%02x", $0) }.joined(),
                ] as [String: Any]
            requests.append(payload)
        }
        self.payloads.removeAll()
        self.payloadsSize = 0

        return self.serverSession.sendPayloadsPList(
            payloads: requests, completion: completion)
    }

    private func sendPayload<T: Encodable>(
        data: T, type: String, completion: @escaping (String?) -> Void
    ) {
        //        sendPayloadJson(data: data, type: type, completion: completion)
        sendPayloadPList(data: data, type: type, completion: completion)
    }

    private func actuallySendPayloads(
        completion: @escaping (String?) -> Void
    ) {
        //        actuallySendPayloadsJson(completion: completion)
        actuallySendPayloadsPList(completion: completion)
    }

    private func extractSeries(
        quanititySample: HKQuantitySample, healthStore: HKHealthStore
    ) -> [(DateInterval, HKQuantity)] {
        if quanititySample.count > 1 {
            let objectPredicate = HKQuery.predicateForObject(
                with: quanititySample.uuid)
            let predicate = HKSamplePredicate.quantitySample(
                type: quanititySample.quantityType,
                predicate: objectPredicate)
            let seriesDescriptor =
                HKQuantitySeriesSampleQueryDescriptor(
                    predicate: predicate,
                    options: .orderByQuantitySampleStartDate)
            let asyncSeries = seriesDescriptor.results(for: healthStore)
            var series: [(DateInterval, HKQuantity)] = []
            let semaphore = DispatchSemaphore(value: 0)
            Task {
                for try await entry in asyncSeries {
                    series.append((entry.dateInterval, entry.quantity))
                }
                semaphore.signal()
            }
            semaphore.wait()
            if series.count == 1 {
                assert(quanititySample.count == 1)
                let dateInterval = series[0].0
                let value = series[0].1
                assert(dateInterval.start == quanititySample.startDate)
                assert(
                    dateInterval.start + dateInterval.duration
                        == quanititySample.endDate)
                assert(value == quanititySample.quantity)
                // print("\(Date()) -- ok")
            }
            return series
        } else {
            let series = [
                (
                    DateInterval(
                        start: quanititySample.startDate,
                        end: quanititySample.endDate),
                    quanititySample.quantity
                )
            ]
            return series
        }
    }

    private func exportHeartBeatSeries(
        healthStore: HKHealthStore, heartbeatSeries: HKHeartbeatSeriesSample,
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
        healthStore.execute(heartbeatSeriesQuery)
    }

    private func exportWorkoutRoute(
        healthStore: HKHealthStore, workoutRoute: HKWorkoutRoute,
        completion: @escaping (String?) -> Void
    ) {
        var locations: [CLLocation] = []

        let workoutRouteQuery = HKWorkoutRouteQuery(route: workoutRoute) {
            (query, locationsOrNil, done, error) in
            guard error == nil else {
                return completion(
                    "Failed to run query: \(error?.localizedDescription ?? "WTF")"
                )
            }

            guard let locationsPart = locationsOrNil else {
                fatalError(
                    "*** Invalid State: This can only fail if there was an error. ***"
                )
            }

            locations.append(contentsOf: locationsPart)

            if done != (locations.count == workoutRoute.count) {
                fatalError(
                    "Workout route query issue: \(done) \(locations.count)/\(workoutRoute.count)"
                )
            }
            if locations.count == workoutRoute.count {
                self.sendPayload(
                    data: self.encodeWorkoutRoute(
                        route: workoutRoute, locations: locations),
                    type: "workout_route",
                    completion: completion)
            } else if locations.count > workoutRoute.count {
                fatalError("Too many samples in workout route")
            }
        }
        healthStore.execute(workoutRouteQuery)
    }

    private let sampleQueue = DispatchQueue(
        label: "com.fitness_exporter.exportQueue")

    private func exportSamples(
        healthStore: HKHealthStore,
        samples: [HKSample], completion: @escaping (String?) -> Void
    ) {
        var index = 0

        func processNextSample() {
            sampleQueue.async {
                if index == samples.count {
                    return self.actuallySendPayloads(
                        completion: completion)
                }

                let sample = samples[index]
                self.exportSample(healthStore: healthStore, sample: sample) {
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

    private func encodeQuantity(quantity: HKQuantity) throws -> CQuantity {
        for unit in HealthDataExporter.UNITS {
            if quantity.is(compatibleWith: unit) {
                return CQuantity(
                    unit: unit.unitString,
                    doubleValue: quantity.doubleValue(for: unit))
            }
        }
        CustomLogger.log(
            "[HDE][Error] Can't find a unit for: \(quantity.description)")
        throw ExtractionError.unitParseError(quantity.description)
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

    private func encodeStatistic(statistic: HKStatistics) throws -> CStatistics
    {
        let sources = statistic.sources?.compactMap {
            source -> (key: String, value: HKSource) in
            return (key: source.name, source)
        }.reduce(into: [String: HKSource]()) {
            dict, tuple in
            dict[tuple.key] = tuple.value
        }

        return try CStatistics(
            startDate: statistic.startDate,
            endDate: statistic.endDate,
            quantityType: encodeQuantityType(
                quantityType: statistic.quantityType),
            sources: statistic.sources?.map { encodeSource(source: $0) },
            sourceAverageQuantity: sources?.mapValues {
                try statistic.averageQuantity(for: $0).map {
                    try encodeQuantity(quantity: $0)
                }
            },
            averageQuantity: statistic.averageQuantity().map {
                try encodeQuantity(quantity: $0)
            },
            sourceMaximumQuantity: sources?.mapValues {
                try statistic.maximumQuantity(for: $0).map {
                    try encodeQuantity(quantity: $0)
                }
            },
            maximumQuantity: statistic.maximumQuantity().map {
                try encodeQuantity(quantity: $0)
            },
            sourceMinimumQuantity: sources?.mapValues {
                try statistic.minimumQuantity(for: $0).map {
                    try encodeQuantity(quantity: $0)
                }
            },
            minimumQuantity: statistic.minimumQuantity().map {
                try encodeQuantity(quantity: $0)
            },
            sourceSumQuantity: sources?.mapValues {
                try statistic.sumQuantity(for: $0).map {
                    try encodeQuantity(quantity: $0)
                }
            },
            sumQuantity: statistic.sumQuantity().map {
                try encodeQuantity(quantity: $0)
            },
            sourceDuration: sources?.mapValues {
                try statistic.duration(for: $0).map {
                    try encodeQuantity(quantity: $0)
                }
            },
            duration: statistic.duration().map {
                try encodeQuantity(quantity: $0)
            },
            sourceMostRecentQuantity: sources?.mapValues {
                try statistic.mostRecentQuantity(for: $0).map {
                    try encodeQuantity(quantity: $0)
                }
            },
            mostRecentQuantity: statistic.mostRecentQuantity().map {
                try encodeQuantity(quantity: $0)
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
    ) throws -> CWorkoutConfiguration {
        return try CWorkoutConfiguration(
            activityType: configuration.activityType.rawValue,
            locationType: configuration.locationType.rawValue,
            swimmingLocationType: configuration.swimmingLocationType.rawValue,
            lapLength: configuration.lapLength.map {
                try encodeQuantity(quantity: $0)
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
        throws -> CWorkoutActivity
    {
        return try CWorkoutActivity(
            uuid: activity.uuid,
            startDate: activity.startDate,
            endDate: activity.endDate,
            duration: activity.duration,
            allStatistics: activity.allStatistics.map {
                CWorkoutAllStatisticEntry(
                    quantityType: encodeQuantityType(quantityType: $0),
                    statistic: try encodeStatistic(statistic: $1)
                )
            },
            metadata: activity.metadata?.mapValues({ "\($0)" }),
            workoutConfiguration: try encodeWorkoutConfiguration(
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

    private func encodeWorkout(workout: HKWorkout) throws -> CWorkout {
        return try CWorkout(
            superSample: encodeSample(sample: workout),
            duration: workout.duration,
            workoutActivityType: workout.workoutActivityType.rawValue,
            workoutActivities: workout.workoutActivities.map {
                try encodeWorkoutActivity(activity: $0)
            },
            workoutEvents: workout.workoutEvents?.map {
                encodeWorkoutEvent(event: $0)
            },
            allStatistics: workout.allStatistics.map {
                CWorkoutAllStatisticEntry(
                    quantityType: encodeQuantityType(quantityType: $0),
                    statistic: try encodeStatistic(statistic: $1)
                )
            }
        )
    }

    struct CQuantitySampleSeriesEntry: Codable {
        let dateInterval: DateInterval
        let quantity: CQuantity
    }

    private func encodeQuantitySampleSeriesEntry(
        dateInterval: DateInterval, quantity: HKQuantity
    ) throws -> CQuantitySampleSeriesEntry {
        return CQuantitySampleSeriesEntry(
            dateInterval: dateInterval,
            quantity: try encodeQuantity(quantity: quantity)
        )
    }

    struct CQuantitySample: Codable {
        let superSample: CSample
        let quantity: CQuantity
        let count: Int
        let quantityType: CQuantityType
        let series: [CQuantitySampleSeriesEntry]
    }

    private func encodeQuantitySample(
        quantitySample: HKQuantitySample, series: [(DateInterval, HKQuantity)]
    )
        throws
        -> CQuantitySample
    {
        assert(quantitySample.count == series.count)
        return CQuantitySample(
            superSample: encodeSample(sample: quantitySample),
            quantity: try encodeQuantity(quantity: quantitySample.quantity),
            count: quantitySample.count,
            quantityType: encodeQuantityType(
                quantityType: quantitySample.quantityType),
            series: try series.map {
                try encodeQuantitySampleSeriesEntry(
                    dateInterval: $0.0, quantity: $0.1)
            })
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
        let superSeriesSample: CSeriesSample
        let timeSinceSeriesStart: [Double]
        let precededByGap: [Bool]
    }

    private func encodeHeartbeatSeriesSample(
        heartbeatSeriesSample: HKHeartbeatSeriesSample,
        timeSinceSeriesStart: [Double],
        precededByGap: [Bool]
    ) -> CHeartbeatSeriesSample {
        return CHeartbeatSeriesSample(
            superSeriesSample: encodeSeriesSample(
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

    struct CWorkoutRoute: Codable {
        let superSeriesSample: CSeriesSample
        let locations: [CCLLocation]

    }

    private func encodeWorkoutRoute(
        route: HKWorkoutRoute, locations: [CLLocation]
    ) -> CWorkoutRoute {
        return CWorkoutRoute(
            superSeriesSample: encodeSeriesSample(seriesSample: route),
            locations: locations.map { encodeCLLocation(location: $0) })
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

    struct CClinicalType: Codable {
        let superSampleType: CSampleType
    }

    private func encodeClinicalType(ct: HKClinicalType) -> CClinicalType {
        return CClinicalType(superSampleType: encodeSampleType(st: ct))
    }

    struct CFHIRVersion: Codable {
        let majorVersion: Int
        let minorVersion: Int
        let patchVersion: Int
        let stringRepresentation: String
        let fhirRelease: String

    }

    private func encodeFHIRVersion(version: HKFHIRVersion) -> CFHIRVersion {
        return CFHIRVersion(
            majorVersion: version.majorVersion,
            minorVersion: version.minorVersion,
            patchVersion: version.patchVersion,
            stringRepresentation: version.stringRepresentation,
            fhirRelease: version.fhirRelease.rawValue)
    }

    struct CFHIRResource: Codable {
        let identifier: String
        let fhirVersion: CFHIRVersion
        let resourceType: String
        let sourceURL: URL?
        let data: Data
    }

    private func encodeFHIRResource(fhirResource: HKFHIRResource?)
        -> CFHIRResource?
    {
        guard let fhirResource = fhirResource else { return nil }
        return CFHIRResource(
            identifier: fhirResource.identifier,
            fhirVersion: encodeFHIRVersion(version: fhirResource.fhirVersion),
            resourceType: fhirResource.resourceType.rawValue,
            sourceURL: fhirResource.sourceURL,
            data: fhirResource.data)
    }

    struct CClinicalRecord: Codable {
        let superSample: CSample
        let clinicalType: CClinicalType
        let displayName: String
        let fhirResource: CFHIRResource?
    }

    private func encodeClinicalRecord(clinicalRecord: HKClinicalRecord)
        -> CClinicalRecord
    {
        return CClinicalRecord(
            superSample: encodeSample(sample: clinicalRecord),
            clinicalType: encodeClinicalType(ct: clinicalRecord.clinicalType),
            displayName: clinicalRecord.displayName,
            fhirResource: encodeFHIRResource(
                fhirResource: clinicalRecord.fhirResource))
    }

    struct CStateOfMind: Codable {
        let superSample: CSample
        let associations: [Int]
        let kind: Int
        let labels: [Int]
        let valence: Double
        let valenceClassification: Int
    }

    private func encodeStateOfMind(stateOfMind: HKStateOfMind)
        -> CStateOfMind
    {
        return CStateOfMind(
            superSample: encodeSample(sample: stateOfMind),
            associations: stateOfMind.associations.map { $0.rawValue },
            kind: stateOfMind.kind.rawValue,
            labels: stateOfMind.labels.map { $0.rawValue },
            valence: stateOfMind.valence,
            valenceClassification: stateOfMind.valenceClassification.rawValue)
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
        HKUnit.appleEffortScore(),
        HKUnit.millimeterOfMercury(),
        //            HKUnit.siemen(),
        //            HKUnit.volt(),
        //            HKUnit.internationalUnit(),
        //            HKUnit.pascal(),
    ]
}

class HealthKitManager {
    // Unified sets of permissions used across the app
    private static func unifiedReadTypes() -> Set<HKObjectType> {
        return Set(ExportConstants.getSampleTypesOfInterest().map { $0 as HKObjectType })
    }

    private static func unifiedShareTypes() -> Set<HKSampleType> {
        var share: [HKSampleType] = [
            HKSeriesType.heartbeat(),
        ]
        if let hrv = HKQuantityType.quantityType(forIdentifier: .heartRateVariabilitySDNN) {
            share.append(hrv)
        }
        if let hr = HKQuantityType.quantityType(forIdentifier: .heartRate) {
            share.append(hr)
        }
        return Set(share)
    }

    /// Centralized authorization request. Always asks for the same sets.
    static func requestUnifiedAuthorization(
        startObservers: Bool,
        completion: @escaping (Bool) -> Void
    ) {
        guard HKHealthStore.isHealthDataAvailable() else {
            CustomLogger.log("[HKM][Error] Health data is not available.")
            return completion(false)
        }

        let healthStore = HKHealthStore()
        let readTypes = unifiedReadTypes()
        let shareTypes = unifiedShareTypes()

        healthStore.requestAuthorization(toShare: shareTypes, read: readTypes) {
            okay, error in
            if let error = error {
                CustomLogger.log("[HKM][Error] Error requesting authorization: \(error)")
                return completion(false)
            }
            if !okay {
                CustomLogger.log("[HKM][Error] Don't have permissions")
                return completion(false)
            }
            if startObservers {
                for sampleType in ExportConstants.getSampleTypesOfInterest() {
                    enableBackgroundDelivery(healthStore, sampleType)
                }
            }
            return completion(true)
        }
    }

    static func initialize(
        startObservers: Bool,
        completion: @escaping (Bool) -> Void
    ) {
        // Delegate to the unified flow to ensure consistency
        requestUnifiedAuthorization(startObservers: startObservers, completion: completion)
    }

    private static func enableBackgroundDelivery(
        _ healthStore: HKHealthStore, _ sampleType: HKSampleType
    ) {
        healthStore.enableBackgroundDelivery(
            for: sampleType, frequency: .immediate
        ) { success, error in
            if let error = error {
                CustomLogger.log(
                    "[HKM][Error] Error enabling background delivery for \(sampleType): \(error.localizedDescription)"
                )
            } else {
                if !success {
                    CustomLogger.log(
                        "[HKM][Error] Error enabling background delivery for \(sampleType)"
                    )
                }

                let query = HKObserverQuery(
                    sampleType: sampleType, predicate: nil
                ) {
                    _, completionHandler, error in
                    if let error = error {
                        CustomLogger.log(
                            "[HKM][Error] HKObserver query for \(sampleType) failed: \(error.localizedDescription)"
                        )
                        return
                    }

                    HealthKitManager.observerProcess(sampleType) {
                        completionHandler()
                    }
                }

                healthStore.execute(query)
            }
        }
    }

    private static func observerProcess(
        _ sampleType: HKSampleType,
        completion: @escaping () -> Void
    ) {
        CustomLogger.log("[HKObserver][Info] \(sampleType) started processing")

        let exporter = IncrementalExporter()
        exporter.run(
            sampleTypes: [sampleType],
            batchSize: 60 * 60 * 24 * 3
        ) {
            status in
            CustomLogger.log(
                "[HKObserver][\(status == nil ? "Success" : "Error")] \(sampleType) finished processing with status: \(status ?? "OK")"
            )
            completion()
        }
    }
}
