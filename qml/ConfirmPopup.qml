import QtQuick
import QtQuick.Controls
import QtQuick.Layouts
import app.verfsnext 1.0

Popup {
    id: root
    readonly property VerfsTheme theme: VerfsTheme {}
    property string title: ""
    property string message: ""
    property string confirmText: "OK"
    property bool danger: false
    signal confirmed()

    modal: true
    focus: true
    anchors.centerIn: Overlay.overlay
    width: 420
    padding: 24
    closePolicy: Popup.CloseOnEscape | Popup.CloseOnPressOutside

    Overlay.modal: Rectangle { color: "#99000000" }

    background: Rectangle {
        radius: theme.radius
        color: theme.cardBgRaised
        border.width: 1
        border.color: theme.borderSubtle
    }

    contentItem: ColumnLayout {
        spacing: 12
        Label {
            text: root.title
            color: theme.textPrimary
            font.pixelSize: 17
            font.bold: true
            Layout.fillWidth: true
            wrapMode: Text.WordWrap
        }
        Label {
            text: root.message
            color: theme.textSecondary
            font.pixelSize: 13
            Layout.fillWidth: true
            wrapMode: Text.WordWrap
        }
        RowLayout {
            Layout.topMargin: 8
            Layout.fillWidth: true
            spacing: 8
            Item { Layout.fillWidth: true }
            TealButton {
                text: "Cancel"
                primary: false
                onClicked: root.close()
            }
            TealButton {
                text: root.confirmText
                danger: root.danger
                onClicked: {
                    root.close()
                    root.confirmed()
                }
            }
        }
    }
}
