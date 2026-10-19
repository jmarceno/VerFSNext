import QtQuick
import QtQuick.Controls
import QtQuick.Layouts
import app.verfsnext 1.0

ScrollView {
    id: root
    readonly property VerfsTheme theme: VerfsTheme {}
    required property var controller
    required property FolderPicker picker
    property string pickTarget: ""
    property var check: ({ mountError: "", dataError: "", dataNote: "", dataFree: null })
    readonly property bool changed: mountField.text !== controller.mount_point || dataField.text !== controller.data_dir

    contentWidth: availableWidth
    clip: true

    function recheck() {
        check = JSON.parse(controller.checkLocations(mountField.text, dataField.text))
    }

    function resetFields() {
        mountField.text = controller.mount_point
        dataField.text = controller.data_dir
        recheck()
    }

    Connections {
        target: root.controller
        function onMount_pointChanged() { root.resetFields() }
        function onData_dirChanged() { root.resetFields() }
    }

    Connections {
        target: root.picker
        function onPicked(path) {
            if (root.pickTarget === "mount")
                mountField.text = path
            else if (root.pickTarget === "data")
                dataField.text = path
            if (root.pickTarget.length > 0)
                root.recheck()
            root.pickTarget = ""
        }
    }

    Component.onCompleted: resetFields()

    ConfirmPopup {
        id: confirmMove
        title: "Use the new folders?"
        message: "VerFSNext will stop, switch to the new folders and start again. Files are not moved: a new data folder starts empty unless it already holds VerFSNext data."
        confirmText: "Switch Folders"
        onConfirmed: root.controller.saveLocations(mountField.text, dataField.text)
    }

    ColumnLayout {
        width: root.availableWidth
        spacing: 16

        Card {
            Layout.fillWidth: true
            title: "Folders"
            subtitle: "Your files appear in the VerFSNext folder. The data folder is where VerFSNext keeps them, deduplicated and compressed."

            ColumnLayout {
                width: parent.width
                spacing: 16
                PathField {
                    id: mountField
                    Layout.fillWidth: true
                    label: "VerFSNext folder"
                    error: root.changed ? root.check.mountError : ""
                    hint: root.changed ? "Must be an empty folder." : "Open this folder to use your files."
                    onEdited: root.recheck()
                    onBrowseRequested: {
                        root.pickTarget = "mount"
                        root.picker.pickFiles = false
                        root.picker.prompt = "Pick an empty folder where your files will appear."
                        root.picker.openAt(mountField.text)
                    }
                }
                PathField {
                    id: dataField
                    Layout.fillWidth: true
                    label: "Data folder"
                    error: root.changed ? root.check.dataError : ""
                    note: root.changed ? root.check.dataNote : ""
                    hint: root.check.dataFree !== null && root.check.dataFree !== undefined
                          ? theme.bytes(root.check.dataFree) + " free on this drive." : ""
                    onEdited: root.recheck()
                    onBrowseRequested: {
                        root.pickTarget = "data"
                        root.picker.pickFiles = false
                        root.picker.prompt = "Pick an empty folder (or existing VerFSNext data) on a drive with enough free space."
                        root.picker.openAt(dataField.text)
                    }
                }
                RowLayout {
                    visible: root.changed
                    spacing: 8
                    TealButton {
                        text: "Use These Folders"
                        enabled: !root.controller.busy && root.check.mountError.length === 0 && root.check.dataError.length === 0
                        onClicked: confirmMove.open()
                    }
                    TealButton {
                        text: "Cancel"
                        primary: false
                        onClicked: root.resetFields()
                    }
                }
            }
        }

        Card {
            Layout.fillWidth: true
            title: "Startup"

            RowLayout {
                width: parent.width
                spacing: 16
                ColumnLayout {
                    Layout.fillWidth: true
                    spacing: 3
                    Label {
                        text: "Run in the background"
                        color: theme.textPrimary
                        font.pixelSize: 14
                    }
                    Label {
                        text: "VerFSNext runs as your own background service: it starts when you log in and keeps your folder available even when this app is closed. No administrator password needed."
                        color: theme.textMuted
                        font.pixelSize: 12
                        Layout.fillWidth: true
                        wrapMode: Text.WordWrap
                    }
                }
                TealSwitch {
                    checked: root.controller.service_mode
                    enabled: !root.controller.busy
                    onToggled: {
                        root.controller.setServiceMode(checked)
                        checked = Qt.binding(() => root.controller.service_mode)
                    }
                }
            }

            Rectangle { width: parent.width; height: 1; color: theme.borderSubtle }

            RowLayout {
                width: parent.width
                spacing: 16
                ColumnLayout {
                    Layout.fillWidth: true
                    spacing: 3
                    Label {
                        text: "Show VerFSNext in the tray when I log in"
                        color: theme.textPrimary
                        font.pixelSize: 14
                    }
                    Label {
                        text: root.controller.service_mode
                              ? "Gives you quick access to snapshots, the vault and statistics."
                              : "Without the background service, this also makes your folder available after login."
                        color: theme.textMuted
                        font.pixelSize: 12
                        Layout.fillWidth: true
                        wrapMode: Text.WordWrap
                    }
                }
                TealSwitch {
                    checked: root.controller.autostart
                    onToggled: {
                        root.controller.setAutostartEnabled(checked)
                        checked = Qt.binding(() => root.controller.autostart)
                    }
                }
            }
        }

        Card {
            Layout.fillWidth: true
            title: "Troubleshooting"

            RowLayout {
                visible: root.controller.stale_mount
                width: parent.width
                spacing: 12
                Label {
                    Layout.fillWidth: true
                    text: "Your VerFSNext folder is still attached to a filesystem that stopped unexpectedly. Repairing it detaches the folder; your data is not affected."
                    color: theme.warning
                    font.pixelSize: 13
                    wrapMode: Text.WordWrap
                }
                TealButton {
                    text: "Repair Folder"
                    enabled: !root.controller.busy
                    onClicked: root.controller.repairMount()
                }
            }

            RowLayout {
                width: parent.width
                spacing: 12
                ColumnLayout {
                    Layout.fillWidth: true
                    spacing: 3
                    Label {
                        text: "Configuration file"
                        color: theme.textPrimary
                        font.pixelSize: 14
                    }
                    Label {
                        text: root.controller.config_path
                        color: theme.textMuted
                        font.pixelSize: 12
                        Layout.fillWidth: true
                        elide: Text.ElideMiddle
                    }
                }
                TealButton {
                    text: "Show in Folder"
                    compact: true
                    primary: false
                    onClicked: root.controller.openPath(root.controller.config_path.substring(0, root.controller.config_path.lastIndexOf("/")))
                }
            }

            Label {
                width: parent.width
                text: "Terminal commands such as “verfsnext stats” and “verfsnext snapshot list” use this same configuration."
                color: theme.textDim
                font.pixelSize: 12
                wrapMode: Text.WordWrap
            }
        }
    }
}
