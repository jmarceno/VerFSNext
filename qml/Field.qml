import QtQuick
import QtQuick.Controls
import QtQuick.Layouts
import app.verfsnext 1.0

ColumnLayout {
    id: root
    readonly property VerfsTheme theme: VerfsTheme {}
    property string label: ""
    property string hint: ""
    property string error: ""
    property alias text: input.text
    property alias placeholderText: input.placeholderText
    property alias echoMode: input.echoMode
    property alias input: input
    signal accepted()

    spacing: 6

    Label {
        visible: root.label.length > 0
        text: root.label
        color: theme.textSecondary
        font.pixelSize: 13
        font.bold: true
    }

    TextField {
        id: input
        Layout.fillWidth: true
        Layout.preferredHeight: 40
        font.pixelSize: 14
        color: theme.textPrimary
        placeholderTextColor: theme.textDim
        selectionColor: theme.accentMuted
        selectedTextColor: theme.textPrimary
        leftPadding: 12
        rightPadding: 12
        onAccepted: root.accepted()
        background: Rectangle {
            radius: theme.radiusSm
            color: theme.inputBg
            border.width: 1
            border.color: root.error.length > 0 ? theme.danger
                        : (input.activeFocus ? theme.accent : theme.borderSubtle)
        }
    }

    Label {
        visible: root.error.length > 0 || root.hint.length > 0
        Layout.fillWidth: true
        text: root.error.length > 0 ? root.error : root.hint
        color: root.error.length > 0 ? theme.dangerStrong : theme.textMuted
        font.pixelSize: 12
        wrapMode: Text.WordWrap
    }
}
