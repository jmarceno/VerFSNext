import QtQuick
import QtQuick.Controls
import QtQuick.Layouts
import app.verfsnext 1.0

Rectangle {
    id: root
    readonly property VerfsTheme theme: VerfsTheme {}
    property string label: ""
    property string value: "—"
    property string caption: ""
    property color valueColor: theme.textPrimary

    radius: theme.radius
    color: theme.cardBg
    border.width: 1
    border.color: "#1f242d"
    implicitHeight: col.implicitHeight + 32
    implicitWidth: 150

    ColumnLayout {
        id: col
        anchors.left: parent.left
        anchors.right: parent.right
        anchors.top: parent.top
        anchors.margins: 16
        spacing: 4

        Label {
            text: root.label.toUpperCase()
            color: theme.textDim
            font.pixelSize: 11
            font.bold: true
            font.letterSpacing: 0.8
            Layout.fillWidth: true
            elide: Text.ElideRight
        }
        Label {
            text: root.value
            color: root.valueColor
            font.pixelSize: 22
            font.bold: true
            Layout.fillWidth: true
            elide: Text.ElideRight
        }
        Label {
            visible: root.caption.length > 0
            text: root.caption
            color: theme.textMuted
            font.pixelSize: 12
            Layout.fillWidth: true
            wrapMode: Text.WordWrap
        }
    }
}
