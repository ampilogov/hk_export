import SwiftUI
import WidgetKit

@main
struct fitness_exporterLiveActivityBundle: WidgetBundle {
    var body: some Widget {
        if #available(iOS 16.1, *) {
            ContinuousRecordingLiveActivity()
        }
    }
}

