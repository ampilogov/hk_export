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

func loadServerCertificate() -> SecCertificate? {
    guard
        let certPath = Bundle.main.path(
            forResource: "server_cert", ofType: "der")
    else {
        CustomLogger.log("Failed to find server_cert.der in bundle")
        return nil
    }
    guard let certData = try? Data(contentsOf: URL(fileURLWithPath: certPath))
    else {
        CustomLogger.log("Failed to load data from server_cert.der")
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

func loadClientCertificate() -> URLCredential? {
    guard
        let certPath = Bundle.main.path(
            forResource: "client_cert", ofType: "p12"),
        let certData = try? Data(contentsOf: URL(fileURLWithPath: certPath))
    else {
        CustomLogger.log("Failed to load client.p12 from bundle")
        return nil
    }

    let password = ""
    let options: [String: Any] = [
        kSecImportExportPassphrase as String: password
    ]

    var items: CFArray?
    let status = SecPKCS12Import(
        certData as CFData, options as CFDictionary, &items)

    guard status == errSecSuccess, let item = (items as? [[String: Any]])?.first
    else {
        CustomLogger.log("Failed to import client certificate")
        return nil
    }

    let identity = item[kSecImportItemIdentity as String] as! SecIdentity
    return URLCredential(
        identity: identity, certificates: nil, persistence: .forSession)
}

class CustomSessionDelegate: NSObject, URLSessionDelegate {
    func urlSession(
        _ session: URLSession, didReceive challenge: URLAuthenticationChallenge,
        completionHandler: @escaping (
            URLSession.AuthChallengeDisposition, URLCredential?
        ) -> Void
    ) {
        if challenge.protectionSpace.authenticationMethod
            == NSURLAuthenticationMethodClientCertificate
        {
            // Handle Client Certificate Authentication
            guard let clientCredential = loadClientCertificate() else {
                CustomLogger.log("Failed to load client certificate")
                completionHandler(.cancelAuthenticationChallenge, nil)
                return
            }
            completionHandler(.useCredential, clientCredential)
            return
        } else if challenge.protectionSpace.authenticationMethod
            == NSURLAuthenticationMethodServerTrust
        {
            guard let serverTrust = challenge.protectionSpace.serverTrust else {
                CustomLogger.log("Failed to get server trust")
                completionHandler(.cancelAuthenticationChallenge, nil)
                return
            }

            guard let myServerCertificate = loadServerCertificate() else {
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
                    SecCertificateCopyData(myServerCertificate) as Data

                if serverCertificateData == localCertificateData {
                    let credential = URLCredential(trust: serverTrust)
                    completionHandler(.useCredential, credential)
                    return
                }
            }

            CustomLogger.log("Certificate not trusted")
            completionHandler(.cancelAuthenticationChallenge, nil)
        }

        // Default case: reject any other challenges
        CustomLogger.log(
            "Unhandled authentication method: \(challenge.protectionSpace.authenticationMethod)"
        )
        completionHandler(.performDefaultHandling, nil)
    }
}

class ServerSession {
    private var server: String
    private var session: URLSession

    private static let LOCK = NSLock()
    private static var SESSIONS: [String: (ServerSession, Date)] = [:]
    private static var TTL: TimeInterval = 60

    static func getSession(server: String) -> ServerSession {
        LOCK.lock()
        defer { LOCK.unlock() }

        let now = Date()
        if let entry = SESSIONS[server], entry.1 + TTL > now {
            return entry.0
        }

        let session = ServerSession(server: server)
        SESSIONS[server] = (session, now)
        return session
    }

    private init(server: String) {
        self.server = server

        let configuration = URLSessionConfiguration.default
        configuration.requestCachePolicy = .reloadIgnoringLocalCacheData
        configuration.urlCache = nil
        configuration.httpShouldSetCookies = true
        configuration.httpShouldUsePipelining = true
        self.session = URLSession(
            configuration: configuration, delegate: CustomSessionDelegate(),
            delegateQueue: nil)
    }

    func sendPayloadsJson(
        payloads: [[String: Any]], timeout: TimeInterval? = 20,
        completion: @escaping (String?) -> Void
    ) {
        if let url = URL(string: server) {
            var request = URLRequest(url: url.appendingPathComponent("batch"))
            request.httpMethod = "POST"
            if let timeout = timeout {
                request.timeoutInterval = timeout
            }
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
                    return completion(
                        "Server error: \(String(describing: response))")
                }
                // CustomLogger.log("Sent")
                return completion(nil)
            }
            // CustomLogger.log("Sending")
            task.resume()
        } else {
            return completion("Invalid URL: \(server)batch")
        }
    }

    func sendPayloadsPList(
        payloads: [[String: Any]], timeout: TimeInterval? = 20,
        completion: @escaping (String?) -> Void
    ) {
        if let url = URL(string: server) {
            var request = URLRequest(url: url.appendingPathComponent("batch"))
            request.httpMethod = "POST"
            if let timeout = timeout {
                request.timeoutInterval = timeout
            }
            request.setValue(
                "application/x-plist", forHTTPHeaderField: "Content-Type")
            request.setValue("gzip", forHTTPHeaderField: "Content-Encoding")
            do {
                let payload = try PropertyListSerialization.data(
                    fromPropertyList: ["payloads": payloads],
                    format: .binary,
                    options: 0
                )
                if let compressedData = compress(data: payload) {
                    request.httpBody = compressedData
                } else {
                    return completion("Failed to compress data")
                }
            } catch {
                CustomLogger.log("Error encoding combined Plist data: \(error)")
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
                    return completion(
                        "Server error: \(String(describing: response))")
                }
                // CustomLogger.log("Sent")
                return completion(nil)
            }
            // CustomLogger.log("Sending")
            task.resume()
        } else {
            return completion("Invalid URL: \(server)batch")
        }
    }

    //    func sendPayloads(
    //        payloads: [[String: Any]], completion: @escaping (String?) -> Void
    //    ) {
    //        sendPayloadsJson(payloads: payloads, completion: completion)
    //    }

    func testConnection(
        timeout: TimeInterval?,
        completion: @escaping (String?) -> Void
    ) {
        if let url = URL(string: server) {
            var request = URLRequest(url: url.appendingPathComponent("status"))
            request.httpMethod = "GET"
            if let timeout = timeout {
                request.timeoutInterval = timeout
            }
            let task = self.session.dataTask(with: request) {
                data, response, error in
                if let error = error {
                    return completion(
                        "Client error: \(error.localizedDescription)")
                }
                guard let httpResponse = response as? HTTPURLResponse,
                    (200...299).contains(httpResponse.statusCode)
                else {
                    return completion(
                        "Server error: \(String(describing: response))")
                }
                return completion(nil)
            }
            task.resume()
        } else {
            return completion("Invalid URL: \(server)status/")
        }
    }
}

final class AutoServerDiscovery {
    private static let RUN_LOCK = NSLock()
    static func run(completion: @escaping (URL?) -> Void) {
        if AutoServerDiscovery.RUN_LOCK.try() {
            let server =
                UserDefaults.standard.string(
                    forKey: UserDefaultsKeys.SERVER_URL) ?? ""
            CustomLogger.log(
                "[ASD][Info] Starting auto server discovery, from \(server)")

            let discovery = AutoServerDiscovery(oldServerURL: server)
            discovery.discoverNewServer { errMsg, url in
                defer { AutoServerDiscovery.RUN_LOCK.unlock() }
                if errMsg == nil {
                    CustomLogger.log(
                        "[ASD][Success] Found a new server: \(url!)")
                    UserDefaults.standard.set(
                        url!.absoluteString, forKey: UserDefaultsKeys.SERVER_URL
                    )
                    completion(url)
                } else {
                    CustomLogger.log(
                        "[ASD][Error] Couldn't find a new server: \(errMsg!)")
                    completion(nil)
                }
            }
        } else {
            CustomLogger.log("[ASD][Error] Can't aquire auto discovery lock")
        }
    }

    private let oldServerURL: String
    private let timeout: TimeInterval
    private let concurrencyLevel: Int
    private let sessionDelegate: URLSessionDelegate

    private init(
        oldServerURL: String,
        timeout: TimeInterval = 1.0,
        concurrencyLevel: Int = 100
    ) {
        self.oldServerURL = oldServerURL
        self.timeout = timeout
        self.concurrencyLevel = concurrencyLevel
        self.sessionDelegate = CustomSessionDelegate()
    }

    func discoverNewServer(completion: @escaping (String?, URL?) -> Void) {
        guard let oldServerURL = URL(string: self.oldServerURL)
        else {
            return completion("Invalid base URL: \(self.oldServerURL)", nil)
        }

        // CustomLogger.log("Base URL: \(oldServerURL)")

        var candidateIPs: [String] = []

        for ifAddress in getIFAddresses() {
            let ips = getAllIPs(
                ipAddress: ifAddress.ip, subnetMask: ifAddress.netmask)
            if ips == nil || ips!.count <= 1 || ips!.count > 256 * 256 {
                continue
            }
            candidateIPs.append(contentsOf: ips!)
        }

        let candidates = candidateIPs.compactMap {
            AutoServerDiscovery.replaceHost(url: oldServerURL, newHost: $0)
        }
        if candidates.isEmpty {
            return completion("No valid IP addresses found", nil)
        }

        // CustomLogger.log("\(candidates)")

        let semaphore = DispatchSemaphore(value: concurrencyLevel)
        let group = DispatchGroup()

        var discoveredServer: URL? = nil
        let discoveredServerLock = NSLock()

        for candidate in candidates {
            if discoveredServer != nil { break }

            group.enter()
            semaphore.wait()

            self.probe(url: candidate) { isReachable in
                if isReachable {
                    discoveredServerLock.lock()
                    if discoveredServer == nil {
                        discoveredServer = candidate
                    }
                    discoveredServerLock.unlock()
                }

                semaphore.signal()
                group.leave()
            }
        }

        group.notify(queue: .main) {
            return completion(
                discoveredServer == nil ? "Not found" : nil, discoveredServer)
        }
    }

    private func probe(url: URL, completion: @escaping (Bool) -> Void) {
        let config = URLSessionConfiguration.ephemeral
        // config.timeoutIntervalForRequest = timeout
        // config.timeoutIntervalForResource = timeout

        let session = URLSession(
            configuration: config, delegate: sessionDelegate, delegateQueue: nil
        )

        var request = URLRequest(url: url.appendingPathComponent("status"))
        request.httpMethod = "GET"
        request.timeoutInterval = timeout

        let task = session.dataTask(with: request) { _, response, _ in
            if let httpResponse = response as? HTTPURLResponse,
                (200..<300).contains(httpResponse.statusCode)
            {
                completion(true)
            } else {
                completion(false)
            }
        }
        task.resume()
    }

    static private func replaceHost(url: URL, newHost: String) -> URL? {
        var components = URLComponents(url: url, resolvingAgainstBaseURL: false)
        components?.host = newHost
        return components?.url
    }
}

enum IPAddress {
    case ipv4(UInt32)
    case ipv6([UInt8])

    init?(from string: String) {
        if let ipv4 = IPAddress.parseIPv4(string: string) {
            self = .ipv4(ipv4)
        } else if let ipv6 = IPAddress.parseIPv6(string: string) {
            self = .ipv6(ipv6)
        } else {
            return nil
        }
    }

    static func parseIPv4(string: String) -> UInt32? {
        let parts = string.split(separator: ".").compactMap { UInt8($0) }
        guard parts.count == 4 else { return nil }
        return (UInt32(parts[0]) << 24) | (UInt32(parts[1]) << 16)
            | (UInt32(parts[2]) << 8) | UInt32(parts[3])
    }

    //    static func parseIPv6(string: String) -> [UInt8]? {
    //        var address = [UInt8](repeating: 0, count: 16)
    //        let scanner = Scanner(string: string)
    //        var value: UInt64 = 0
    //        var index = 0
    //        while !scanner.isAtEnd && index < 16 {
    //            if scanner.scanHexInt64(&value) {
    //                address[index] = UInt8((value & 0xFF00) >> 8)
    //                address[index + 1] = UInt8(value & 0xFF)
    //                index += 2
    //            }
    //            scanner.scanString(":", into: nil)
    //        }
    //        return index == 16 ? address : nil
    //    }

    static func parseIPv6(string: String) -> [UInt8]? {
        var address = [UInt8](repeating: 0, count: 16)
        let scanner = Scanner(string: string)
        scanner.charactersToBeSkipped = nil
        var index = 0

        while !scanner.isAtEnd && index < 16 {
            if let hexString = scanner.scanUpToCharacters(
                from: CharacterSet(charactersIn: ":")),
                let hexValue = UInt64(hexString, radix: 16)
            {
                if index + 1 < 16 {
                    address[index] = UInt8((hexValue & 0xFF00) >> 8)
                    address[index + 1] = UInt8(hexValue & 0xFF)
                    index += 2
                } else {
                    return nil
                }
            } else {
                return nil
            }
            _ = scanner.scanCharacters(from: CharacterSet(charactersIn: ":"))
        }

        return index == 16 ? address : nil
    }

    func toString() -> String {
        switch self {
        case .ipv4(let addr):
            return [
                String((addr >> 24) & 0xFF),
                String((addr >> 16) & 0xFF),
                String((addr >> 8) & 0xFF),
                String(addr & 0xFF),
            ].joined(separator: ".")
        case .ipv6(let addr):
            return stride(from: 0, to: 16, by: 2).map {
                String(format: "%02x%02x", addr[$0], addr[$0 + 1])
            }.joined(separator: ":")
        }
    }
}

func getAllIPs(ipAddress: String, subnetMask: String) -> [String]? {
    guard let ip = IPAddress(from: ipAddress),
        let mask = IPAddress(from: subnetMask)
    else { return nil }
    switch (ip, mask) {
    case (.ipv4(let ipAddr), .ipv4(let maskAddr)):
        let network = ipAddr & maskAddr
        let broadcast = network | ~maskAddr
        guard broadcast - network < 1_000_000 else { return nil }
        return (network...broadcast).map { IPAddress.ipv4($0).toString() }
    case (.ipv6(let ipAddr), .ipv6(let maskAddr)):
        let network = zip(ipAddr, maskAddr).map { $0 & $1 }
        // Note: Enumerating IPv6 addresses is impractical; returning network address only
        return [IPAddress.ipv6(network).toString()]
    default:
        return nil
    }
}

struct NetInfo {
    let ip: String
    let netmask: String
}

func getIFAddresses() -> [NetInfo] {
    var addresses = [NetInfo]()

    // Get list of all interfaces on the local machine:
    var ifaddr: UnsafeMutablePointer<ifaddrs>? = nil
    if getifaddrs(&ifaddr) == 0 {

        var ptr = ifaddr
        while ptr != nil {

            let flags = Int32((ptr?.pointee.ifa_flags)!)
            var addr = ptr?.pointee.ifa_addr.pointee

            // Check for running IPv4, IPv6 interfaces. Skip the loopback interface.
            if (flags & (IFF_UP | IFF_RUNNING | IFF_LOOPBACK))
                == (IFF_UP | IFF_RUNNING)
            {
                if addr?.sa_family == UInt8(AF_INET)
                    || addr?.sa_family == UInt8(AF_INET6)
                {

                    // Convert interface address to a human readable string:
                    var hostname = [CChar](repeating: 0, count: Int(NI_MAXHOST))
                    if getnameinfo(
                        &addr!, socklen_t((addr?.sa_len)!), &hostname,
                        socklen_t(hostname.count),
                        nil, socklen_t(0), NI_NUMERICHOST) == 0
                    {
                        if let address = String.init(validatingUTF8: hostname) {

                            var net = ptr?.pointee.ifa_netmask.pointee
                            var netmaskName = [CChar](
                                repeating: 0, count: Int(NI_MAXHOST))
                            getnameinfo(
                                &net!, socklen_t((net?.sa_len)!), &netmaskName,
                                socklen_t(netmaskName.count),
                                nil, socklen_t(0), NI_NUMERICHOST)  // == 0
                            if let netmask = String.init(
                                validatingUTF8: netmaskName)
                            {
                                addresses.append(
                                    NetInfo(ip: address, netmask: netmask))
                            }
                        }
                    }
                }
            }
            ptr = ptr?.pointee.ifa_next
        }
        freeifaddrs(ifaddr)
    }
    return addresses
}
