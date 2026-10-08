import QtQuick
import QtQuick.Controls
import QtQuick.Layouts
import app.verfsnext 1.0

Rectangle {
    id: root
    readonly property VerfsTheme theme: VerfsTheme {}
    required property var controller
    readonly property string daemonState: controller.daemon_state
    readonly property var snapshots: controller.snapshots_json.length > 0 ? JSON.parse(controller.snapshots_json) : []

    color: theme.windowBg
    Layout.fillHeight: true
    Layout.preferredWidth: theme.sidebarWidth

    ColumnLayout {
        anchors.fill: parent
        anchors.margins: 18
        anchors.topMargin: 8
        spacing: 18

        Rectangle {
            Layout.fillWidth: true
            radius: theme.radius
            color: theme.cardBg
            implicitHeight: statusCol.implicitHeight + 28

            ColumnLayout {
                id: statusCol
                anchors.left: parent.left
                anchors.right: parent.right
                anchors.top: parent.top
                anchors.margins: 14
                spacing: 8

                RowLayout {
                    spacing: 10
                    StatusDot {
                        dotColor: theme.stateColor(root.daemonState)
                        pulse: root.daemonState === "starting" || root.daemonState === "stopping"
                    }
                    Label {
                        text: theme.stateLabel(root.daemonState)
                        color: theme.textPrimary
                        font.pixelSize: 14
                        font.bold: true
                        Layout.fillWidth: true
                    }
                }
                Label {
                    visible: root.controller.configured
                    text: root.controller.mount_point
                    color: theme.textMuted
                    font.pixelSize: 12
                    elide: Text.ElideMiddle
                    Layout.fillWidth: true
                }
                TealButton {
                    visible: root.controller.configured
                    Layout.fillWidth: true
                    compact: true
                    primary: root.daemonState !== "running"
                    enabled: !root.controller.busy && root.daemonState !== "starting" && root.daemonState !== "stopping"
                    text: root.daemonState === "running" ? "Stop" : "Start"
                    onClicked: root.daemonState === "running" ? root.controller.stopDaemon() : root.controller.startDaemon()
                }
            }
        }

        ColumnLayout {
            spacing: 4
            Layout.fillWidth: true

            NavItem {
                Layout.fillWidth: true
                label: "Overview"
                icon: "overview"
                selected: root.controller.control_tab === 0
                onClicked: root.controller.selectTab(0)
            }
            NavItem {
                Layout.fillWidth: true
                label: "Snapshots"
                icon: "snapshots"
                badge: root.snapshots.length > 0 ? String(root.snapshots.length) : ""
                selected: root.controller.control_tab === 1
                onClicked: root.controller.selectTab(1)
            }
            NavItem {
                Layout.fillWidth: true
                label: "Vault"
                icon: "vault"
                selected: root.controller.control_tab === 2
                onClicked: root.controller.selectTab(2)
            }
            NavItem {
                Layout.fillWidth: true
                label: "Folders & Startup"
                icon: "folders"
                selected: root.controller.control_tab === 3
                onClicked: root.controller.selectTab(3)
            }
            NavItem {
                Layout.fillWidth: true
                label: "Settings"
                icon: "settings"
                selected: root.controller.control_tab === 4
                onClicked: root.controller.selectTab(4)
            }
        }

        Item { Layout.fillHeight: true }

        Label {
            Layout.fillWidth: true
            text: root.controller.service_mode
                  ? "Runs in the background. Closing this window keeps your folder available."
                  : "Runs while this app is open. Closing this window keeps it in the tray."
            color: theme.textDim
            font.pixelSize: 12
            wrapMode: Text.WordWrap
        }
    }
}
