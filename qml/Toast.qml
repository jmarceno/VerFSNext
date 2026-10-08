import QtQuick
import QtQuick.Controls
import QtQuick.Layouts
import app.verfsnext 1.0

Rectangle {
    id: root
    readonly property VerfsTheme theme: VerfsTheme {}
    required property var controller

    width: Math.min(480, parent.width - 48)
    implicitHeight: row.implicitHeight + 24
    radius: theme.radius
    color: theme.cardBgRaised
    border.width: 1
    border.color: controller.toast_error ? theme.danger : theme.accentMuted
    opacity: 0
    visible: opacity > 0

    Behavior on opacity { NumberAnimation { duration: 180 } }

    Connections {
        target: root.controller
        function onToast_serialChanged() {
            root.opacity = 1
            hideTimer.interval = Math.max(root.controller.toast_error ? 10000 : 5000,
                                          root.controller.toast.length * 70)
            hideTimer.restart()
        }
    }

    Timer {
        id: hideTimer
        onTriggered: root.opacity = 0
    }

    RowLayout {
        id: row
        anchors.left: parent.left
        anchors.right: parent.right
        anchors.verticalCenter: parent.verticalCenter
        anchors.margins: 14
        spacing: 12

        Rectangle {
            Layout.alignment: Qt.AlignTop
            Layout.topMargin: 4
            width: 8
            height: 8
            radius: 4
            color: root.controller.toast_error ? theme.danger : theme.accent
        }
        Label {
            Layout.fillWidth: true
            text: root.controller.toast
            color: theme.textPrimary
            font.pixelSize: 13
            wrapMode: Text.WordWrap
            textFormat: Text.PlainText
        }
        Label {
            Layout.alignment: Qt.AlignTop
            text: "✕"
            color: theme.textMuted
            font.pixelSize: 12
            MouseArea {
                anchors.fill: parent
                anchors.margins: -6
                cursorShape: Qt.PointingHandCursor
                onClicked: root.opacity = 0
            }
        }
    }
}
