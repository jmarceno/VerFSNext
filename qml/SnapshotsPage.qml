import QtQuick
import QtQuick.Controls
import QtQuick.Layouts
import app.verfsnext 1.0

ColumnLayout {
    id: root
    readonly property VerfsTheme theme: VerfsTheme {}
    required property var controller
    readonly property bool running: controller.daemon_state === "running"
    readonly property var names: {
        const list = controller.snapshots_json.length > 0 ? JSON.parse(controller.snapshots_json) : []
        return list.slice().sort().reverse()
    }

    function defaultName() {
        return Qt.formatDateTime(new Date(), "yyyy-MM-dd_hh-mm-ss")
    }

    spacing: 16

    ConfirmPopup {
        id: confirmDelete
        property string target: ""
        title: "Delete “" + target + "”?"
        message: "The snapshot disappears from .snapshots. Files in your folder are not touched, and space only used by this snapshot is reclaimed during the next cleanup."
        confirmText: "Delete Snapshot"
        danger: true
        onConfirmed: root.controller.deleteSnapshot(target)
    }

    Card {
        Layout.fillWidth: true
        title: "Take a snapshot"
        subtitle: "A snapshot is a read-only copy of your whole folder at this moment. It takes no extra space until files change, and you can browse it anytime in the .snapshots folder."

        RowLayout {
            width: parent.width
            spacing: 10
            Field {
                id: nameField
                Layout.fillWidth: true
                placeholderText: "Name (optional — date and time if empty)"
                enabled: root.running
                onAccepted: takeBtn.clicked()
            }
            TealButton {
                id: takeBtn
                Layout.alignment: Qt.AlignTop
                text: "Take Snapshot"
                enabled: root.running && !root.controller.busy
                onClicked: {
                    const name = nameField.text.trim()
                    root.controller.createSnapshot(name.length > 0 ? name : root.defaultName())
                    nameField.text = ""
                }
            }
        }
    }

    Card {
        Layout.fillWidth: true
        Layout.fillHeight: true
        title: root.names.length > 0 ? (root.names.length === 1 ? "1 snapshot" : root.names.length + " snapshots") : "Your snapshots"

        Label {
            visible: !root.running || root.names.length === 0
            width: parent.width
            text: !root.running ? "Start VerFSNext to see and manage your snapshots."
                  : (root.controller.snapshots_loading ? "Loading…" : "No snapshots yet. Take your first one above, or from the tray icon at any time.")
            color: theme.textMuted
            font.pixelSize: 13
            wrapMode: Text.WordWrap
        }

        Repeater {
            model: root.running ? root.names : []
            delegate: Rectangle {
                required property string modelData
                width: parent ? parent.width : 0
                height: 52
                radius: theme.radiusSm
                color: rowMouse.containsMouse ? theme.cardBgRaised : theme.inputBg

                MouseArea {
                    id: rowMouse
                    anchors.fill: parent
                    hoverEnabled: true
                }

                RowLayout {
                    anchors.fill: parent
                    anchors.leftMargin: 14
                    anchors.rightMargin: 10
                    spacing: 10
                    Icon {
                        name: "snapshots"
                        color: theme.accent
                        Layout.preferredWidth: 18
                        Layout.preferredHeight: 18
                    }
                    Label {
                        text: modelData
                        color: theme.textPrimary
                        font.pixelSize: 14
                        elide: Text.ElideMiddle
                        Layout.fillWidth: true
                    }
                    TealButton {
                        text: "Open"
                        compact: true
                        primary: false
                        onClicked: root.controller.openSnapshot(modelData)
                    }
                    TealButton {
                        text: "Delete"
                        compact: true
                        danger: true
                        enabled: !root.controller.busy
                        onClicked: {
                            confirmDelete.target = modelData
                            confirmDelete.open()
                        }
                    }
                }
            }
        }
    }
}
