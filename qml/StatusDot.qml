import QtQuick
import app.verfsnext 1.0

Rectangle {
    id: root
    readonly property VerfsTheme theme: VerfsTheme {}
    property color dotColor: theme.statusGreen
    property bool pulse: false
    width: 10
    height: 10
    radius: width / 2
    color: root.dotColor

    SequentialAnimation on opacity {
        running: root.pulse
        loops: Animation.Infinite
        onStopped: root.opacity = 1
        NumberAnimation { to: 0.35; duration: 700; easing.type: Easing.InOutQuad }
        NumberAnimation { to: 1.0; duration: 700; easing.type: Easing.InOutQuad }
    }
}
