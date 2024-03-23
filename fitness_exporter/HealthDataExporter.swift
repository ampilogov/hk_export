import Foundation
import HealthKit

enum ExtractionError: Error {
    case unitParseError(String)
}

class HealthDataExporter {
    private var healthStore: HKHealthStore
    
    init(healthStore: HKHealthStore) {
        self.healthStore = healthStore
    }
    
    func exportWorkouts(from startDate: Date, to endDate: Date, server: String, completion: @escaping (String) -> Void) {
        print("Exporting data to \(server), from \(startDate) till \(endDate)")
        
        let predicate = HKQuery.predicateForSamples(withStart: startDate, end: endDate, options: .strictStartDate)
        let sortDescriptor = NSSortDescriptor(key: HKSampleSortIdentifierStartDate, ascending: true)
        
        let query = HKSampleQuery(sampleType: HKObjectType.workoutType(), predicate: predicate, limit: HKObjectQueryNoLimit, sortDescriptors: [sortDescriptor]) { (query, samples, error) in
            guard let workouts = samples as? [HKWorkout], error == nil else {
                completion("Failed to fetch workouts: \(error?.localizedDescription ?? "Unknown error")")
                return
            }
            
            let workoutsData = self.encodeWorkouts(workouts: workouts)
            let encoder = JSONEncoder()
            do {
                let jsonData = try encoder.encode(workoutsData)
                let session = URLSession(configuration: .default)
                if let url = URL(string: server) {
                    var request = URLRequest(url: url)
                    request.httpMethod = "POST"
                    request.setValue("application/json", forHTTPHeaderField: "Content-Type")
                    request.httpBody = jsonData
                    
                    let task = session.dataTask(with: request) { data, response, error in
                        if let error = error {
                            print("Client error: \(error.localizedDescription)")
                            return
                        }
                        guard let httpResponse = response as? HTTPURLResponse,
                              (200...299).contains(httpResponse.statusCode) else {
                            print("Server error")
                            return
                        }
                        if let data = data, let dataString = String(data: data, encoding: .utf8) {
                            print("Server response: \(dataString)")
                        }
                    }
                    task.resume()
                }
                completion("Success.")
            } catch {
                completion("Failed to serialize data: \(error)")
                return;
            }
        }
        
        healthStore.execute(query)
    }
    
    struct CObjectType: Codable {
        var identifier: String
    }
    
    private func encodeObjectType(ot: HKObjectType) -> CObjectType {
        return CObjectType(
            identifier: ot.identifier
        )
    }
    
    struct CSampleType : Codable {
        var superObjectType : CObjectType
        var isMinimumDurationRestricted: Bool
        var minimumAllowedDuration: TimeInterval
        var isMaximumDurationRestricted: Bool
        var maximumAllowedDuration: TimeInterval
        var allowsRecalibrationForEstimates: Bool

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
    
    struct CQuantityType : Codable {
        var superSampleType : CSampleType
        var aggregationStyle: Int
    }

    private func encodeQuantityType(quantityType: HKQuantityType) -> CQuantityType {
        return CQuantityType(superSampleType: encodeSampleType(st: quantityType), aggregationStyle: quantityType.aggregationStyle.rawValue)
    }
    
    struct CDevice: Codable {
        var udiDeviceIdentifier: String?
        var firmwareVersion: String?
        var hardwareVersion: String?
        var localIdentifier: String?
        var manufacturer: String?
        var model: String?
        var name: String?
        var softwareVersion: String?
    }
    
    private func encodeDevice(device: HKDevice) -> CDevice {
        return CDevice(
            udiDeviceIdentifier: device.udiDeviceIdentifier,
            firmwareVersion: device.firmwareVersion,
            hardwareVersion: device.hardwareVersion,
            localIdentifier: device.localIdentifier,
            manufacturer: device.manufacturer,
            model: device.model,
            name:device.name,
            softwareVersion:device.softwareVersion
        )
    }
    
    struct COperationSystemVersion : Codable {
        var majorVersion: Int
        var minorVersion: Int
        var patchVersion: Int
    }
    
    private func encodeOperationSystemVersion(osv: OperatingSystemVersion) -> COperationSystemVersion {
        return COperationSystemVersion(
            majorVersion:osv.majorVersion,
            minorVersion: osv.minorVersion,
            patchVersion: osv.patchVersion
        )
    }
    
    struct CSource : Codable {
        var bundleIdentifier: String
        var name: String
    }
    
    private func encodeSource(source: HKSource) -> CSource{
        return CSource(bundleIdentifier: source.bundleIdentifier, name: source.name)
    }

    struct CSourceRevision : Codable {
        var source: CSource
        var version: String?
        var operatingSystemVersion: COperationSystemVersion
        var productType: String?
    }

    private func encodeSourceRevision(sr: HKSourceRevision) -> CSourceRevision {
        return CSourceRevision(
            source: encodeSource(source: sr.source),
            version: sr.version,
            operatingSystemVersion: encodeOperationSystemVersion(osv: sr.operatingSystemVersion),
            productType: sr.productType
        )
    }
    
    struct CObject: Codable {
        var uuid: UUID
        var metadata: [String : String]?
        var device: CDevice?
        var sourceRevision: CSourceRevision
    }
    
    private func encodeObject(object: HKObject) -> CObject {
        return CObject(uuid: object.uuid,
                       metadata: object.metadata?.mapValues({"\($0)"}),
                       device: object.device.map { encodeDevice(device: $0) },
                       sourceRevision: encodeSourceRevision(sr: object.sourceRevision)
        )
    }
    
    struct CSample : Codable {
        var superObject : CObject
        var startDate: Date
        var endDate: Date
        var hasUndeterminedDuration: Bool
        var sampleType: CSampleType
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
    
    struct CQuantity : Codable {
        var unit: String
        var doubleValue: Double
    }
    
    private func encodeQuantity(quantity: HKQuantity) -> CQuantity {
        for unit in [
            HKUnit.gram(),
            HKUnit.meter(),
            HKUnit.liter(),
            HKUnit.pascal(),
            HKUnit.second(),
            HKUnit.joule(),
            HKUnit.watt(),
            HKUnit.degreeCelsius(),
            HKUnit.decibelHearingLevel(),
            HKUnit.hertz(),
            HKUnit.diopter(),
            HKUnit.degreeAngle(),
            HKUnit.siemen(),
            HKUnit.volt(),
            HKUnit.internationalUnit(),
            HKUnit.count(),
            HKUnit.percent(),
            HKUnit.meter().unitDivided(by: HKUnit.second())
        ] {
            if (quantity.is(compatibleWith: unit)) {
                return CQuantity(unit: unit.unitString, doubleValue: quantity.doubleValue(for: unit))
            }
        }
        print("Can't find a unit for: \(quantity.description)")
        return CQuantity(unit: "unknown: \(quantity.description)", doubleValue: Double.nan)
    }
    
    struct CStatistics : Codable {
        var startDate: Date
        var endDate: Date
        var quantityType: CQuantityType
        var sources: [CSource]?
        var sourceAverageQuantity: [String: CQuantity?]?
        var averageQuantity: CQuantity?
        var sourceMaximumQuantity: [String: CQuantity?]?
        var maximumQuantity: CQuantity?
        var sourceMinimumQuantity: [String: CQuantity?]?
        var minimumQuantity: CQuantity?
        var sourceSumQuantity: [String: CQuantity?]?
        var sumQuantity: CQuantity?
        var sourceDuration: [String: CQuantity?]?
        var duration: CQuantity?
        var sourceMostRecentQuantity: [String: CQuantity?]?
        var mostRecentQuantity: CQuantity?
        var sourceMostRecentQuantityDateInterval: [String: DateInterval?]?
        var mostRecentQuantityDateInterval: DateInterval?
    }
    
    private func encodeStatistic(statistic: HKStatistics) -> CStatistics {
        let sources = statistic.sources?.compactMap{
            source -> (key: String, value: HKSource) in return (key: source.name, source)
        }.reduce(into: [String: HKSource]()) {
            dict, tuple in
            dict[tuple.key] = tuple.value
        }

        return CStatistics(
            startDate: statistic.startDate,
            endDate: statistic.endDate,
            quantityType: encodeQuantityType(quantityType: statistic.quantityType),
            sources: statistic.sources?.map{encodeSource(source: $0)},
            sourceAverageQuantity: sources?.mapValues{statistic.averageQuantity(for: $0).map{encodeQuantity(quantity: $0)}},
            averageQuantity: statistic.averageQuantity().map{encodeQuantity(quantity: $0)},
            sourceMaximumQuantity: sources?.mapValues{statistic.maximumQuantity(for: $0).map{encodeQuantity(quantity: $0)}},
            maximumQuantity: statistic.maximumQuantity().map{encodeQuantity(quantity: $0)},
            sourceMinimumQuantity: sources?.mapValues{statistic.minimumQuantity(for: $0).map{encodeQuantity(quantity: $0)}},
            minimumQuantity: statistic.minimumQuantity().map{encodeQuantity(quantity: $0)},
            sourceSumQuantity: sources?.mapValues{statistic.sumQuantity(for: $0).map{encodeQuantity(quantity: $0)}},
            sumQuantity: statistic.sumQuantity().map{encodeQuantity(quantity: $0)},
            sourceDuration: sources?.mapValues{statistic.duration(for: $0).map{encodeQuantity(quantity: $0)}},
            duration: statistic.duration().map{encodeQuantity(quantity: $0)},
            sourceMostRecentQuantity: sources?.mapValues{statistic.mostRecentQuantity(for: $0).map{encodeQuantity(quantity: $0)}},
            mostRecentQuantity: statistic.mostRecentQuantity().map{encodeQuantity(quantity: $0)},
            sourceMostRecentQuantityDateInterval: sources?.mapValues{statistic.mostRecentQuantityDateInterval(for: $0)},
            mostRecentQuantityDateInterval: statistic.mostRecentQuantityDateInterval()
        )
    }
    
    struct CWorkoutAllStatisticEntry : Codable {
        var quantityType : CQuantityType
        var statistic : CStatistics
    }
    
    struct CWorkoutConfiguration : Codable {
        var activityType: UInt
        var locationType: Int
        var swimmingLocationType: Int
        var lapLength: CQuantity?

    }
    
    private func encodeWorkoutConfiguration(configuration : HKWorkoutConfiguration) -> CWorkoutConfiguration {
        return CWorkoutConfiguration(
            activityType: configuration.activityType.rawValue,
            locationType: configuration.locationType.rawValue,
            swimmingLocationType: configuration.swimmingLocationType.rawValue,
            lapLength: configuration.lapLength.map{encodeQuantity(quantity: $0)})
    }
    
    struct CWorkoutEvent : Codable {
        var dateInterval: DateInterval
        var type: Int
        var metadata: [String : String]?
    }
    
    private func encodeWorkoutEvent(event: HKWorkoutEvent) -> CWorkoutEvent {
        return CWorkoutEvent(
            dateInterval: event.dateInterval,
            type: event.type.rawValue,
            metadata: event.metadata?.mapValues({"\($0)"})
        )
    }

    struct CWorkoutActivity : Codable {
        var uuid: UUID
        var startDate: Date
        var endDate: Date?
        var duration: TimeInterval
        var allStatistics: [CWorkoutAllStatisticEntry]
        var metadata: [String : String]?
        var workoutConfiguration: CWorkoutConfiguration
        var workoutEvents: [CWorkoutEvent]
    }
    
    private func encodeWorkoutActivity(activity: HKWorkoutActivity) -> CWorkoutActivity {
        return CWorkoutActivity(
            uuid: activity.uuid,
            startDate: activity.startDate,
            endDate: activity.endDate,
            duration: activity.duration,
            allStatistics: activity.allStatistics.map { CWorkoutAllStatisticEntry(
                quantityType: encodeQuantityType(quantityType: $0),
                statistic: encodeStatistic(statistic: $1)
            )},
            metadata: activity.metadata?.mapValues({"\($0)"}),
            workoutConfiguration: encodeWorkoutConfiguration(configuration: activity.workoutConfiguration),
            workoutEvents: activity.workoutEvents.map{encodeWorkoutEvent(event: $0)})
    }
        
    struct CWorkout : Codable {
        var superSample : CSample
        var duration: TimeInterval
        var workoutActivityType: UInt
        var workoutActivities: [CWorkoutActivity]
        var workoutEvents: [CWorkoutEvent]?
        var allStatistics: [CWorkoutAllStatisticEntry]
    }
    
    private func encodeWorkout(workout: HKWorkout) -> CWorkout {
        return CWorkout(
            superSample: encodeSample(sample: workout),
            duration: workout.duration,
            workoutActivityType: workout.workoutActivityType.rawValue,
            workoutActivities: workout.workoutActivities.map{encodeWorkoutActivity(activity: $0)},
            workoutEvents: workout.workoutEvents?.map{encodeWorkoutEvent(event: $0)},
            allStatistics: workout.allStatistics.map { CWorkoutAllStatisticEntry(
                quantityType: encodeQuantityType(quantityType: $0),
                statistic: encodeStatistic(statistic: $1)
            )}
        )
    }
    
    struct CWorkouts : Codable {
        var workouts : [CWorkout]
    }
    
    private func encodeWorkouts(workouts: [HKWorkout]) -> CWorkouts {
        return CWorkouts(workouts: workouts.map{encodeWorkout(workout: $0)})
    }
}
