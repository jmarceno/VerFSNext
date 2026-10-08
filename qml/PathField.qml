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
    property string note: ""
    property alias text: input.text
    property string buttonText: "Choose…"
    signal browseRequested()
    signal edited()

    spacing: 6

    Label {
        visible: root.label.length > 0
        text: root.label
        color: theme.textSecondary
        font.pixelSize: 13
        font.bold: true
    }

    RowLayout {
        Layout.fillWidth: true
        spacing: 8

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
            onTextEdited: root.edited()
            background: Rectangle {
                radius: theme.radiusSm
                color: theme.inputBg
                border.width: 1
                border.color: root.error.length > 0 ? theme.danger
                            : (input.activeFocus ? theme.accent : theme.borderSubtle)
            }
        }

        TealButton {
            text: root.buttonText
            primary: false
            onClicked: root.browseRequested()
        }
    }

    Label {
        visible: text.length > 0
        Layout.fillWidth: true
        text: root.error.length > 0 ? root.error : (root.note.length > 0 ? root.note : root.hint)
        color: root.error.length > 0 ? theme.dangerStrong : (root.note.length > 0 ? theme.accentStrong : theme.textMuted)
        font.pixelSize: 12
        wrapMode: Text.WordWrap
    }
}
