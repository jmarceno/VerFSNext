import QtQuick
import QtQuick.Controls
import QtQuick.Layouts
import QtQuick.Window
import app.verfsnext 1.0

// First-run assistant: welcome, how to run, settings, folders, start.
Window {
    id: root
    readonly property VerfsTheme theme: VerfsTheme {}
    required property var controller
    readonly property int stepCount: 5
    property int step: 0
    property bool useService: true
    property bool customize: false
    property bool autostart: true
    property string pickTarget: ""
    property var check: ({ mountError: "", dataError: "", dataNote: "", dataFree: null })
    readonly property var defaults: controller.setup_settings_json.length > 0 ? JSON.parse(controller.setup_settings_json) : []
    readonly property bool locationsOk: check.mountError.length === 0 && check.dataError.length === 0

    width: 780
    height: 640
    minimumWidth: 700
    minimumHeight: 600
    visible: controller.setup_visible
    color: theme.windowBg
    title: "Set Up VerFSNext"
    flags: Qt.Window | Qt.FramelessWindowHint

    function valueOf(key) {
        for (const f of defaults) {
            if (f.key === key)
                return key in editor.edits ? editor.edits[key] : f.value
        }
        return undefined
    }

    function recheck() {
        check = JSON.parse(controller.checkLocations(mountField.text, dataField.text))
    }

    function canContinue() {
        switch (step) {
        case 2: return editor.valid
        case 3: return locationsOk
        default: return true
        }
    }

    function next() {
        if (step === 3)
            recheck()
        if (step < stepCount - 1 && canContinue())
            step++
    }

    onStepChanged: {
        if (step === 3)
            recheck()
    }

    onVisibleChanged: {
        if (visible) {
            raise()
            requestActivate()
        }
    }

    onClosing: (close) => {
        close.accepted = false
        controller.setup_visible = false
    }

    Component.onCompleted: {
        mountField.text = controller.suggested_mount
        dataField.text = controller.suggested_data
    }

    Connections {
        target: root.controller
        function onSuggested_mountChanged() { mountField.text = root.controller.suggested_mount }
        function onSuggested_dataChanged() { dataField.text = root.controller.suggested_data }
    }

    FolderPicker {
        id: picker
        controller: root.controller
        onPicked: (path) => {
            if (root.pickTarget === "mount")
                mountField.text = path
            else
                dataField.text = path
            root.recheck()
        }
    }

    ColumnLayout {
        anchors.fill: parent
        spacing: 0

        TitleBar {
            Layout.fillWidth: true
            window: root
            title: "Set Up VerFSNext"
            canMaximize: false
            onCloseRequested: root.controller.setup_visible = false
        }

        StackLayout {
            Layout.fillWidth: true
            Layout.fillHeight: true
            Layout.leftMargin: 48
            Layout.rightMargin: 48
            Layout.topMargin: 12
            currentIndex: root.step

            // 0 — Welcome
            ColumnLayout {
                spacing: 22

                // fillWidth: without it every child has a fixed maximum width,
                // so the StackLayout would shrink this page and nothing centers.
                Item { Layout.fillWidth: true; Layout.fillHeight: true; Layout.maximumHeight: 24 }
                Image {
                    Layout.alignment: Qt.AlignHCenter
                    source: theme.iconSource
                    sourceSize.width: 192
                    sourceSize.height: 192
                    Layout.preferredWidth: 96
                    Layout.preferredHeight: 96
                }
                ColumnLayout {
                    Layout.alignment: Qt.AlignHCenter
                    spacing: 6
                    Label {
                        Layout.alignment: Qt.AlignHCenter
                        text: "Welcome to VerFSNext"
                        color: theme.textPrimary
                        font.pixelSize: 30
                        font.bold: true
                    }
                    Label {
                        Layout.alignment: Qt.AlignHCenter
                        text: "A smarter folder for your files."
                        color: theme.textMuted
                        font.pixelSize: 15
                    }
                }

                ColumnLayout {
                    Layout.alignment: Qt.AlignHCenter
                    Layout.fillWidth: false
                    Layout.preferredWidth: 520
                    Layout.maximumWidth: 520
                    Layout.topMargin: 8
                    spacing: 18

                    Repeater {
                        model: [
                            ["space", "Saves space automatically", "Identical files, and identical parts of files, are stored only once, and everything is compressed."],
                            ["snapshots", "Go back in time", "Snapshots keep read-only copies of your folder that take almost no extra space."],
                            ["vault", "Private when you need it", "An encrypted vault keeps chosen files locked behind your password."]
                        ]
                        delegate: RowLayout {
                            required property var modelData
                            Layout.fillWidth: true
                            spacing: 16
                            Rectangle {
                                Layout.alignment: Qt.AlignTop
                                width: 40
                                height: 40
                                radius: 10
                                color: theme.accentSoft
                                Icon {
                                    anchors.centerIn: parent
                                    width: 20
                                    height: 20
                                    name: modelData[0]
                                    color: theme.accentStrong
                                }
                            }
                            ColumnLayout {
                                Layout.fillWidth: true
                                spacing: 3
                                Label {
                                    text: modelData[1]
                                    color: theme.textPrimary
                                    font.pixelSize: 15
                                    font.bold: true
                                }
                                Label {
                                    Layout.fillWidth: true
                                    text: modelData[2]
                                    color: theme.textSecondary
                                    font.pixelSize: 13
                                    wrapMode: Text.WordWrap
                                }
                            }
                        }
                    }
                }
                Item { Layout.fillHeight: true }
            }

            // 1 — How to run
            ColumnLayout {
                spacing: 16
                Label {
                    text: "How should VerFSNext run?"
                    color: theme.textPrimary
                    font.pixelSize: 24
                    font.bold: true
                }
                Label {
                    Layout.fillWidth: true
                    text: "Either way, everything stays under your user account. No administrator password is needed."
                    color: theme.textMuted
                    font.pixelSize: 14
                    wrapMode: Text.WordWrap
                }
                ChoiceCard {
                    Layout.fillWidth: true
                    Layout.topMargin: 8
                    title: "In the background"
                    badge: "RECOMMENDED"
                    description: "VerFSNext is set up as your own background service. It starts when you log in and keeps your folder available even when this window is closed."
                    checked: root.useService
                    onClicked: root.useService = true
                }
                ChoiceCard {
                    Layout.fillWidth: true
                    title: "Only while the app is open"
                    description: "VerFSNext starts with this app and your folder disconnects when you quit it. You can switch to the background service later."
                    checked: !root.useService
                    onClicked: root.useService = false
                }
                Item { Layout.fillHeight: true }
            }

            // 2 — Settings
            ColumnLayout {
                spacing: 14
                Label {
                    text: root.customize ? "Customize settings" : "Recommended settings"
                    color: theme.textPrimary
                    font.pixelSize: 24
                    font.bold: true
                }
                Label {
                    Layout.fillWidth: true
                    text: root.customize
                          ? "Change what you like. Everything except the options marked “fixed after setup” can be changed later in Settings."
                          : "These work well for most people. Is this how you'd like VerFSNext to work? You can change it anytime in Settings."
                    color: theme.textMuted
                    font.pixelSize: 14
                    wrapMode: Text.WordWrap
                }

                Card {
                    visible: !root.customize
                    Layout.fillWidth: true

                    Repeater {
                        model: [
                            ["Compression", root.valueOf("zstd_compression_level") < 0 ? "Fast — favors speed" : "Level " + root.valueOf("zstd_compression_level")],
                            ["Duplicate detection", "On, for every file"],
                            ["Memory for caching", root.valueOf("chunk_cache_capacity_mb") + " MB"],
                            ["Changes saved to disk", "Every " + root.valueOf("sync_interval_ms") + " seconds"],
                            ["Space from deleted files", "Reclaimed after " + root.valueOf("gc_idle_min_ms") + " quiet seconds"],
                            ["Other people on this computer", root.valueOf("fuse_allow_other") ? "Can open your folder" : "Can't open your folder"],
                            ["Encrypted vault", root.valueOf("vault_enabled") ? "Available when you want it" : "Turned off"]
                        ]
                        delegate: Column {
                            required property var modelData
                            required property int index
                            width: parent ? parent.width : 0
                            spacing: 12
                            Rectangle {
                                visible: index > 0
                                width: parent.width
                                height: 1
                                color: theme.borderSubtle
                            }
                            RowLayout {
                                width: parent.width
                                Label {
                                    text: modelData[0]
                                    color: theme.textSecondary
                                    font.pixelSize: 14
                                    Layout.fillWidth: true
                                }
                                Label {
                                    text: modelData[1]
                                    color: theme.textPrimary
                                    font.pixelSize: 14
                                    font.bold: true
                                }
                            }
                        }
                    }
                }

                ScrollView {
                    id: editorScroll
                    visible: root.customize
                    Layout.fillWidth: true
                    Layout.fillHeight: true
                    contentWidth: availableWidth
                    clip: true
                    SettingsEditor {
                        id: editor
                        width: editorScroll.availableWidth
                        setupMode: true
                        modelJson: root.controller.setup_settings_json
                    }
                }

                RowLayout {
                    visible: !root.customize
                    spacing: 10
                    TealButton {
                        text: "Customize…"
                        primary: false
                        onClicked: root.customize = true
                    }
                }
                RowLayout {
                    visible: root.customize
                    spacing: 10
                    TealButton {
                        text: "Use Recommended Settings"
                        primary: false
                        compact: true
                        onClicked: {
                            editor.reset()
                            root.customize = false
                        }
                    }
                }
                Item { Layout.fillHeight: !root.customize }
            }

            // 3 — Folders
            ColumnLayout {
                spacing: 16
                Label {
                    text: "Where should things live?"
                    color: theme.textPrimary
                    font.pixelSize: 24
                    font.bold: true
                }
                Label {
                    Layout.fillWidth: true
                    text: "Folders that don't exist yet will be created for you."
                    color: theme.textMuted
                    font.pixelSize: 14
                    wrapMode: Text.WordWrap
                }
                Card {
                    Layout.fillWidth: true
                    Layout.topMargin: 4
                    ColumnLayout {
                        width: parent.width
                        spacing: 20
                        PathField {
                            id: mountField
                            Layout.fillWidth: true
                            label: "Your VerFSNext folder"
                            error: root.check.mountError
                            hint: "Your files appear here. Use it like any other folder."
                            onEdited: root.recheck()
                            onBrowseRequested: {
                                root.pickTarget = "mount"
                                picker.pickFiles = false
                                picker.prompt = "Pick an empty folder, or create a new one, where your files will appear."
                                picker.openAt(mountField.text)
                            }
                        }
                        PathField {
                            id: dataField
                            Layout.fillWidth: true
                            label: "Data folder"
                            error: root.check.dataError
                            note: root.check.dataNote
                            hint: "VerFSNext keeps your files here, deduplicated and compressed. Pick a drive with plenty of free space"
                                  + (root.check.dataFree !== null && root.check.dataFree !== undefined
                                     ? " (" + theme.bytes(root.check.dataFree) + " free here)." : ".")
                            onEdited: root.recheck()
                            onBrowseRequested: {
                                root.pickTarget = "data"
                                picker.pickFiles = false
                                picker.prompt = "Pick an empty folder (or existing VerFSNext data) on a drive with enough free space."
                                picker.openAt(dataField.text)
                            }
                        }
                    }
                }
                Item { Layout.fillHeight: true }
            }

            // 4 — Ready
            ColumnLayout {
                spacing: 16
                Label {
                    text: root.controller.setup_running ? "Setting things up…" : "Ready to go"
                    color: theme.textPrimary
                    font.pixelSize: 24
                    font.bold: true
                }
                Label {
                    Layout.fillWidth: true
                    text: root.controller.setup_running
                          ? "Creating your folders and starting VerFSNext. This only takes a moment."
                          : "Here's what will happen. When VerFSNext is running, this window closes and VerFSNext waits in the tray."
                    color: theme.textMuted
                    font.pixelSize: 14
                    wrapMode: Text.WordWrap
                }
                Card {
                    Layout.fillWidth: true
                    Repeater {
                        model: [
                            ["Your folder", mountField.text],
                            ["Data folder", dataField.text],
                            ["Runs", root.useService ? "In the background, from login" : "While the app is open"],
                            ["Settings", Object.keys(editor.edits).length === 0 ? "Recommended" : Object.keys(editor.edits).length + " customized"]
                        ]
                        delegate: RowLayout {
                            required property var modelData
                            width: parent ? parent.width : 0
                            spacing: 16
                            Label {
                                text: modelData[0]
                                color: theme.textSecondary
                                font.pixelSize: 14
                                Layout.preferredWidth: 120
                            }
                            Label {
                                text: modelData[1]
                                color: theme.textPrimary
                                font.pixelSize: 14
                                font.bold: true
                                elide: Text.ElideMiddle
                                Layout.fillWidth: true
                            }
                        }
                    }
                }
                RowLayout {
                    Layout.fillWidth: true
                    spacing: 14
                    TealSwitch {
                        checked: root.autostart
                        enabled: !root.controller.setup_running
                        onToggled: root.autostart = checked
                    }
                    ColumnLayout {
                        Layout.fillWidth: true
                        spacing: 2
                        Label {
                            text: "Show VerFSNext in the tray when I log in"
                            color: theme.textPrimary
                            font.pixelSize: 14
                        }
                        Label {
                            Layout.fillWidth: true
                            text: "Quick access to snapshots, the vault and statistics. VerFSNext is also added to your applications menu."
                            color: theme.textMuted
                            font.pixelSize: 12
                            wrapMode: Text.WordWrap
                        }
                    }
                }
                RowLayout {
                    visible: root.controller.setup_running
                    spacing: 10
                    Spinner {}
                    Label {
                        text: "Starting VerFSNext…"
                        color: theme.textSecondary
                        font.pixelSize: 13
                    }
                }
                Rectangle {
                    visible: root.controller.setup_error.length > 0 && !root.controller.setup_running
                    Layout.fillWidth: true
                    Layout.fillHeight: true
                    Layout.maximumHeight: 200
                    radius: theme.radiusSm
                    color: theme.dangerBg
                    ColumnLayout {
                        anchors.fill: parent
                        anchors.margins: 12
                        spacing: 6
                        Label {
                            text: "Something went wrong"
                            color: theme.dangerStrong
                            font.pixelSize: 14
                            font.bold: true
                        }
                        ScrollView {
                            Layout.fillWidth: true
                            Layout.fillHeight: true
                            clip: true
                            TextArea {
                                readOnly: true
                                text: root.controller.setup_error
                                color: theme.textSecondary
                                font.pixelSize: 12
                                font.family: "monospace"
                                wrapMode: Text.WrapAnywhere
                                selectByMouse: true
                                background: null
                                padding: 0
                            }
                        }
                    }
                }
                Item { Layout.fillHeight: true }
            }
        }

        // Footer
        Rectangle {
            Layout.fillWidth: true
            height: 1
            color: theme.borderSubtle
        }
        RowLayout {
            Layout.fillWidth: true
            Layout.leftMargin: 48
            Layout.rightMargin: 48
            Layout.topMargin: 16
            Layout.bottomMargin: 18
            spacing: 10

            StepDots {
                count: root.stepCount
                current: root.step
            }
            Item { Layout.fillWidth: true }
            TealButton {
                visible: root.step > 0
                text: "Back"
                primary: false
                enabled: !root.controller.setup_running
                onClicked: root.step--
            }
            TealButton {
                visible: root.step < root.stepCount - 1
                text: root.step === 0 ? "Get Started" : (root.step === 2 && !root.customize ? "Use These Settings" : "Continue")
                enabled: root.canContinue()
                onClicked: root.next()
            }
            TealButton {
                visible: root.step === root.stepCount - 1
                text: root.controller.setup_error.length > 0 ? "Try Again" : "Start VerFSNext"
                enabled: !root.controller.setup_running
                onClicked: root.controller.finishSetup(mountField.text, dataField.text, editor.editsJson(), root.useService, root.autostart)
            }
        }
    }

    Rectangle {
        anchors.fill: parent
        color: "transparent"
        border.width: 1
        border.color: theme.borderSubtle
    }
}
