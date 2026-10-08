import QtQuick
import QtQuick.Controls
import QtQuick.Layouts
import QtQuick.Window
import app.verfsnext 1.0

Window {
    id: root
    readonly property VerfsTheme theme: VerfsTheme {}
    required property var controller
    readonly property var titles: [
        ["Overview", "How your folder is doing."],
        ["Snapshots", "Moments in time you can always go back to."],
        ["Vault", "Encrypted space for private files."],
        ["Folders & Startup", "Where things live and how VerFSNext starts."],
        ["Settings", "Tune how VerFSNext saves space and uses your computer."]
    ]

    width: 1080
    height: 720
    minimumWidth: 900
    minimumHeight: 600
    visible: controller.control_visible
    color: theme.windowBg
    title: "VerFSNext"
    flags: Qt.Window | Qt.FramelessWindowHint

    onVisibleChanged: {
        if (visible) {
            raise()
            requestActivate()
        }
    }

    onClosing: (close) => {
        close.accepted = false
        controller.hideControl()
    }

    FolderPicker {
        id: picker
        controller: root.controller
    }

    ColumnLayout {
        anchors.fill: parent
        spacing: 0

        TitleBar {
            Layout.fillWidth: true
            window: root
            title: "VerFSNext"
            onCloseRequested: root.controller.hideControl()
        }

        RowLayout {
            Layout.fillWidth: true
            Layout.fillHeight: true
            spacing: 0

            Sidebar {
                controller: root.controller
            }

            ColumnLayout {
                Layout.fillWidth: true
                Layout.fillHeight: true
                Layout.margins: 24
                Layout.topMargin: 8
                spacing: 16

                ColumnLayout {
                    spacing: 4
                    Layout.fillWidth: true
                    Label {
                        text: root.titles[root.controller.control_tab][0]
                        color: theme.textPrimary
                        font.pixelSize: 28
                        font.bold: true
                    }
                    Label {
                        text: root.titles[root.controller.control_tab][1]
                        color: theme.textMuted
                        font.pixelSize: 13
                    }
                }

                // Banners
                Rectangle {
                    visible: root.controller.config_error.length > 0
                    Layout.fillWidth: true
                    radius: theme.radiusSm
                    color: theme.dangerBg
                    implicitHeight: cfgErr.implicitHeight + 24
                    Label {
                        id: cfgErr
                        anchors.left: parent.left
                        anchors.right: parent.right
                        anchors.verticalCenter: parent.verticalCenter
                        anchors.margins: 12
                        text: "Your configuration can't be used: " + root.controller.config_error
                              + "\nFix " + root.controller.config_path + " and reopen VerFSNext."
                        color: theme.dangerStrong
                        font.pixelSize: 13
                        wrapMode: Text.WordWrap
                    }
                }

                Rectangle {
                    visible: root.controller.restart_needed
                    Layout.fillWidth: true
                    radius: theme.radiusSm
                    color: theme.accentSoft
                    border.width: 1
                    border.color: theme.accentMuted
                    implicitHeight: restartRow.implicitHeight + 20
                    RowLayout {
                        id: restartRow
                        anchors.left: parent.left
                        anchors.right: parent.right
                        anchors.verticalCenter: parent.verticalCenter
                        anchors.margins: 12
                        spacing: 12
                        Label {
                            Layout.fillWidth: true
                            text: "Your new settings are saved. Restart VerFSNext to start using them."
                            color: theme.textPrimary
                            font.pixelSize: 13
                            wrapMode: Text.WordWrap
                        }
                        TealButton {
                            text: "Restart Now"
                            compact: true
                            enabled: !root.controller.busy
                            onClicked: root.controller.restartDaemon()
                        }
                    }
                }

                Rectangle {
                    visible: root.controller.busy
                    Layout.fillWidth: true
                    radius: theme.radiusSm
                    color: theme.cardBg
                    implicitHeight: 40
                    RowLayout {
                        anchors.fill: parent
                        anchors.leftMargin: 12
                        anchors.rightMargin: 12
                        spacing: 10
                        Spinner {}
                        Label {
                            Layout.fillWidth: true
                            text: root.controller.busy_text
                            color: theme.textSecondary
                            font.pixelSize: 13
                            elide: Text.ElideRight
                        }
                    }
                }

                StackLayout {
                    Layout.fillWidth: true
                    Layout.fillHeight: true
                    currentIndex: root.controller.control_tab

                    OverviewPage {
                        controller: root.controller
                        onSnapshotRequested: root.controller.createSnapshot(Qt.formatDateTime(new Date(), "yyyy-MM-dd_hh-mm-ss"))
                    }
                    SnapshotsPage {
                        controller: root.controller
                    }
                    VaultPage {
                        controller: root.controller
                        picker: picker
                        onOpenSettings: root.controller.selectTab(4)
                    }
                    StoragePage {
                        controller: root.controller
                        picker: picker
                    }
                    SettingsPage {
                        controller: root.controller
                    }
                }
            }
        }
    }

    Toast {
        controller: root.controller
        anchors.right: parent.right
        anchors.top: parent.top
        anchors.rightMargin: 24
        anchors.topMargin: 56
    }

    // Resize handles (the window is frameless).
    MouseArea {
        anchors.right: parent.right
        anchors.top: parent.top
        anchors.bottom: parent.bottom
        anchors.bottomMargin: 12
        width: 5
        cursorShape: Qt.SizeHorCursor
        onPressed: root.startSystemResize(Qt.RightEdge)
    }
    MouseArea {
        anchors.left: parent.left
        anchors.top: parent.top
        anchors.bottom: parent.bottom
        width: 5
        cursorShape: Qt.SizeHorCursor
        onPressed: root.startSystemResize(Qt.LeftEdge)
    }
    MouseArea {
        anchors.left: parent.left
        anchors.right: parent.right
        anchors.bottom: parent.bottom
        anchors.rightMargin: 12
        height: 5
        cursorShape: Qt.SizeVerCursor
        onPressed: root.startSystemResize(Qt.BottomEdge)
    }
    MouseArea {
        anchors.right: parent.right
        anchors.bottom: parent.bottom
        width: 12
        height: 12
        cursorShape: Qt.SizeFDiagCursor
        onPressed: root.startSystemResize(Qt.RightEdge | Qt.BottomEdge)
    }

    // Thin frame: frameless windows otherwise blend into dark desktops.
    Rectangle {
        anchors.fill: parent
        color: "transparent"
        border.width: 1
        border.color: theme.borderSubtle
    }
}
