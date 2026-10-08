import QtQuick
import app.verfsnext 1.0

Row {
    id: root
    readonly property VerfsTheme theme: VerfsTheme {}
    property int count: 5
    property int current: 0
    spacing: 8

    Repeater {
        model: root.count
        delegate: Rectangle {
            required property int index
            width: index === root.current ? 22 : 8
            height: 8
            radius: 4
            color: index <= root.current ? theme.accent : theme.sliderTrack
            Behavior on width { NumberAnimation { duration: 160; easing.type: Easing.OutCubic } }
        }
    }
}
