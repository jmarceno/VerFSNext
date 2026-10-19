import QtQuick
import QtQuick.Controls
import QtQuick.Layouts
import app.verfsnext 1.0

Item {
    id: root
    readonly property VerfsTheme theme: VerfsTheme {}
    property string label: ""
    property string icon: ""
    property bool selected: false
    property string badge: ""
    signal clicked()

    implicitHeight: 40

    Rectangle {
        anchors.fill: parent
        radius: theme.radiusSm
        color: root.selected ? theme.accentSoft : (mouse.containsMouse ? "#1a1e25" : "transparent")

        Rectangle {
            visible: root.selected
            anchors.left: parent.left
            anchors.top: parent.top
            anchors.bottom: parent.bottom
            anchors.topMargin: 8
            anchors.bottomMargin: 8
            width: 3
            radius: 1.5
            color: theme.accent
        }

        RowLayout {
            anchors.fill: parent
            anchors.leftMargin: 14
            anchors.rightMargin: 12
            spacing: 12

            Icon {
                name: root.icon
                color: root.selected ? theme.accent : theme.textMuted
                Layout.preferredWidth: 18
                Layout.preferredHeight: 18
            }
            Label {
                text: root.label
                color: root.selected ? theme.accentStrong : theme.textPrimary
                font.pixelSize: 14
                font.bold: root.selected
                Layout.fillWidth: true
            }
            Label {
                visible: root.badge.length > 0
                text: root.badge
                color: theme.textMuted
                font.pixelSize: 12
            }
        }
    }

    MouseArea {
        id: mouse
        anchors.fill: parent
        hoverEnabled: true
        cursorShape: Qt.PointingHandCursor
        onClicked: root.clicked()
    }
}
