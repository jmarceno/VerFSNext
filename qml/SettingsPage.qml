import QtQuick
import QtQuick.Controls
import QtQuick.Layouts
import app.verfsnext 1.0

ColumnLayout {
    id: root
    readonly property VerfsTheme theme: VerfsTheme {}
    required property var controller

    spacing: 12

    ScrollView {
        id: scroll
        Layout.fillWidth: true
        Layout.fillHeight: true
        contentWidth: availableWidth
        clip: true

        SettingsEditor {
            id: editor
            width: scroll.availableWidth
            modelJson: root.controller.settings_json
        }
    }

    Rectangle {
        Layout.fillWidth: true
        radius: theme.radius
        color: theme.cardBgRaised
        implicitHeight: footer.implicitHeight + 24
        visible: editor.dirty || !editor.valid

        RowLayout {
            id: footer
            anchors.left: parent.left
            anchors.right: parent.right
            anchors.verticalCenter: parent.verticalCenter
            anchors.margins: 12
            spacing: 10
            Label {
                Layout.fillWidth: true
                text: !editor.valid ? "Some values aren't valid numbers yet."
                      : (Object.keys(editor.edits).length === 1 ? "1 change" : Object.keys(editor.edits).length + " changes")
                      + (root.controller.daemon_state === "running" && editor.valid ? " · applies after a restart" : "")
                color: editor.valid ? theme.textSecondary : theme.dangerStrong
                font.pixelSize: 13
                wrapMode: Text.WordWrap
            }
            TealButton {
                text: "Revert"
                primary: false
                onClicked: editor.reset()
            }
            TealButton {
                text: "Save"
                enabled: editor.valid && editor.dirty
                onClicked: root.controller.saveSettings(editor.editsJson())
            }
        }
    }
}
