import QtQuick
import app.verfsnext 1.0

// Root is a plain QtObject: windows and timers hang off typed properties.
// QtObject has NO default property, so a bare child object (e.g.
// `Connections {}`) fails the whole file at runtime; bind children to
// properties as below.
QtObject {
    id: root

    property AppController controller: AppController {}

    property SetupWindow setupWindow: SetupWindow {
        controller: root.controller
    }

    property ControlWindow controlWindow: ControlWindow {
        controller: root.controller
    }

    // Drains Rust-side channels (worker results, tray, activation).
    property Timer tickTimer: Timer {
        interval: 100
        running: true
        repeat: true
        onTriggered: root.controller.tick()
    }

    Component.onCompleted: {
        // Tray app: closing windows must not quit.
        Qt.application.quitOnLastWindowClosed = false
        root.controller.bootstrap()
    }
}
