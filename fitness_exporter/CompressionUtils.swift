import Foundation
import Compression

enum CompressionAlgorithm: CaseIterable {
    case lzfse
    case lz4
    case zlib
    case lzma

    var raw: compression_algorithm {
        switch self {
        case .lzfse: return COMPRESSION_LZFSE
        case .lz4: return COMPRESSION_LZ4
        case .zlib: return COMPRESSION_ZLIB
        case .lzma: return COMPRESSION_LZMA
        }
    }
}

extension Data {
    /// Compresses data using Apple Compression framework (lossless).
    /// Defaults to LZFSE which typically offers excellent ratio and speed on iOS.
    func compressed(using algorithm: CompressionAlgorithm = .lzfse) throws -> Data {
        if isEmpty { return Data() }

        var stream = compression_stream()
        var status = compression_stream_init(&stream, COMPRESSION_STREAM_ENCODE, algorithm.raw)
        guard status != COMPRESSION_STATUS_ERROR else {
            throw NSError(domain: "CompressionError", code: -1, userInfo: [NSLocalizedDescriptionKey: "Failed to init compression stream"])
        }
        defer { compression_stream_destroy(&stream) }

        return try withUnsafeBytes { (srcBuffer: UnsafeRawBufferPointer) -> Data in
            guard let srcBase = srcBuffer.baseAddress?.assumingMemoryBound(to: UInt8.self) else {
                return Data()
            }

            let dstChunkSize = 64 * 1024
            var dstBuffer = [UInt8](repeating: 0, count: dstChunkSize)
            var output = Data()

            stream.src_ptr = srcBase
            stream.src_size = count
            stream.dst_ptr = &dstBuffer
            stream.dst_size = dstChunkSize

            while true {
                status = compression_stream_process(&stream, 0)
                switch status {
                case COMPRESSION_STATUS_OK:
                    if stream.dst_size == 0 {
                        output.append(&dstBuffer, count: dstChunkSize)
                        stream.dst_ptr = &dstBuffer
                        stream.dst_size = dstChunkSize
                    }
                    if stream.src_size == 0 {
                        // Finish
                        status = compression_stream_process(&stream, Int32(COMPRESSION_STREAM_FINALIZE.rawValue))
                        if status == COMPRESSION_STATUS_END {
                            let produced = dstChunkSize - stream.dst_size
                            if produced > 0 { output.append(&dstBuffer, count: produced) }
                            return output
                        }
                    }
                case COMPRESSION_STATUS_END:
                    let produced = dstChunkSize - stream.dst_size
                    if produced > 0 { output.append(&dstBuffer, count: produced) }
                    return output
                default:
                    throw NSError(domain: "CompressionError", code: -2, userInfo: [NSLocalizedDescriptionKey: "Compression failed with status \(status)"])
                }
            }
        }
    }
}

