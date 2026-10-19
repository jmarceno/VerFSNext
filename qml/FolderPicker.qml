import QtQuick
import QtQuick.Controls
import QtQuick.Layouts
import app.verfsnext 1.0

// In-app folder / file chooser. Native dialogs depend on host platform
// plugins; this one looks and behaves the same everywhere.
Popup {
    id: root
    readonly property VerfsTheme theme: VerfsTheme {}
    required property var controller
    property bool pickFiles: false
    property string title: pickFiles ? "Choose a File" : "Choose a Folder"
    property string prompt: ""
    property bool showHidden: false
    property var listing: ({ path: "", parent: null, crumbs: [], entries: [], error: "" })
    property string selectedFile: ""
    property bool creating: false
    property string createError: ""
    readonly property var places: controller.places_json.length > 0 ? JSON.parse(controller.places_json) : []
    readonly property var visibleEntries: listing.entries.filter(e => root.showHidden || !e.hidden)
    readonly property string currentName: {
        const crumbs = listing.crumbs
        return crumbs.length > 0 ? crumbs[crumbs.length - 1].name : ""
    }
    signal picked(string path)

    function openAt(path) {
        selectedFile = ""
        creating = false
        createError = ""
        listing = { path: path, parent: null, crumbs: [], entries: [], error: "" }
        root.navigate(path.length > 0 ? path : controller.home_dir)
        open()
    }

    function navigate(path) {
        selectedFile = ""
        controller.browse(path, pickFiles)
    }

    function choose() {
        const path = pickFiles ? selectedFile : listing.path
        if (path.length === 0)
            return
        close()
        picked(path)
    }

    modal: true
    focus: true
    anchors.centerIn: Overlay.overlay
    width: Math.min(760, (parent ? parent.width : 760) - 48)
    height: Math.min(540, (parent ? parent.height : 540) - 48)
    padding: 0
    closePolicy: Popup.CloseOnEscape

    Overlay.modal: Rectangle { color: "#99000000" }

    background: Rectangle {
        radius: theme.radius
        color: theme.cardBg
        border.width: 1
        border.color: theme.borderSubtle
    }

    Connections {
        target: root.controller
        function onBrowser_jsonChanged() {
            if (root.visible)
                root.listing = JSON.parse(root.controller.browser_json)
        }
    }

    contentItem: ColumnLayout {
        spacing: 0

        ColumnLayout {
            Layout.fillWidth: true
            Layout.margins: 20
            Layout.bottomMargin: 12
            spacing: 4
            Label {
                text: root.title
                color: theme.textPrimary
                font.pixelSize: 18
                font.bold: true
            }
            Label {
                visible: root.prompt.length > 0
                text: root.prompt
                color: theme.textMuted
                font.pixelSize: 13
                Layout.fillWidth: true
                wrapMode: Text.WordWrap
            }
        }

        Rectangle { Layout.fillWidth: true; height: 1; color: theme.borderSubtle }

        RowLayout {
            Layout.fillWidth: true
            Layout.fillHeight: true
            spacing: 0

            // Places
            Rectangle {
                Layout.fillHeight: true
                Layout.preferredWidth: 170
                color: theme.windowBg

                ColumnLayout {
                    anchors.fill: parent
                    anchors.margins: 10
                    spacing: 2
                    Label {
                        text: "PLACES"
                        color: theme.textDim
                        font.pixelSize: 11
                        font.bold: true
                        font.letterSpacing: 0.8
                        Layout.leftMargin: 8
                        Layout.bottomMargin: 6
                        Layout.topMargin: 4
                    }
                    Repeater {
                        model: root.places
                        delegate: Rectangle {
                            required property var modelData
                            Layout.fillWidth: true
                            implicitHeight: 32
                            radius: 6
                            readonly property bool active: root.listing.path === modelData.path
                            color: active ? theme.accentSoft : (placeMouse.containsMouse ? theme.cardBgRaised : "transparent")
                            Label {
                                anchors.verticalCenter: parent.verticalCenter
                                anchors.left: parent.left
                                anchors.right: parent.right
                                anchors.leftMargin: 10
                                anchors.rightMargin: 8
                                text: modelData.name
                                color: parent.active ? theme.accentStrong : theme.textSecondary
                                font.pixelSize: 13
                                elide: Text.ElideRight
                            }
                            MouseArea {
                                id: placeMouse
                                anchors.fill: parent
                                hoverEnabled: true
                                cursorShape: Qt.PointingHandCursor
                                onClicked: root.navigate(modelData.path)
                            }
                        }
                    }
                    Item { Layout.fillHeight: true }
                }
            }

            Rectangle { Layout.fillHeight: true; width: 1; color: theme.borderSubtle }

            ColumnLayout {
                Layout.fillWidth: true
                Layout.fillHeight: true
                spacing: 0

                // Breadcrumbs
                RowLayout {
                    Layout.fillWidth: true
                    Layout.margins: 10
                    spacing: 6

                    TealButton {
                        text: "‹"
                        compact: true
                        primary: false
                        enabled: root.listing.parent !== null && root.listing.parent !== undefined
                        onClicked: root.navigate(root.listing.parent)
                    }

                    Flickable {
                        id: crumbFlick
                        Layout.fillWidth: true
                        Layout.preferredHeight: 32
                        clip: true
                        contentWidth: crumbRow.width
                        contentHeight: height
                        flickableDirection: Flickable.HorizontalFlick
                        onContentWidthChanged: contentX = Math.max(0, contentWidth - width)

                        Row {
                            id: crumbRow
                            height: parent.height
                            spacing: 2
                            Repeater {
                                model: root.listing.crumbs
                                delegate: Row {
                                    required property var modelData
                                    required property int index
                                    height: crumbRow.height
                                    spacing: 2
                                    Label {
                                        visible: index > 1
                                        text: "›"
                                        color: theme.textDim
                                        anchors.verticalCenter: parent.verticalCenter
                                    }
                                    Rectangle {
                                        anchors.verticalCenter: parent.verticalCenter
                                        height: 26
                                        width: crumbLabel.implicitWidth + 14
                                        radius: 6
                                        color: crumbMouse.containsMouse ? theme.cardBgRaised : "transparent"
                                        Label {
                                            id: crumbLabel
                                            anchors.centerIn: parent
                                            text: modelData.name
                                            color: index === root.listing.crumbs.length - 1 ? theme.textPrimary : theme.textMuted
                                            font.pixelSize: 13
                                            font.bold: index === root.listing.crumbs.length - 1
                                        }
                                        MouseArea {
                                            id: crumbMouse
                                            anchors.fill: parent
                                            hoverEnabled: true
                                            cursorShape: Qt.PointingHandCursor
                                            onClicked: root.navigate(modelData.path)
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                Rectangle { Layout.fillWidth: true; height: 1; color: theme.borderSubtle }

                // Entries
                Item {
                    Layout.fillWidth: true
                    Layout.fillHeight: true

                    ListView {
                        id: list
                        anchors.fill: parent
                        anchors.margins: 6
                        clip: true
                        model: root.visibleEntries
                        boundsBehavior: Flickable.StopAtBounds
                        ScrollBar.vertical: ScrollBar {}
                        delegate: Rectangle {
                            required property var modelData
                            width: list.width - 12
                            height: 34
                            radius: 6
                            readonly property bool selected: !modelData.isDir && root.selectedFile === modelData.path
                            color: selected ? theme.accentSoft : (entryMouse.containsMouse ? theme.cardBgRaised : "transparent")
                            opacity: modelData.isDir || root.pickFiles ? 1.0 : 0.5
                            RowLayout {
                                anchors.fill: parent
                                anchors.leftMargin: 10
                                anchors.rightMargin: 10
                                spacing: 10
                                Label {
                                    text: modelData.isDir ? "▰" : "▫"
                                    color: modelData.isDir ? theme.accent : theme.textMuted
                                    font.pixelSize: 13
                                    Layout.preferredWidth: 14
                                }
                                Label {
                                    text: modelData.name
                                    color: modelData.hidden ? theme.textMuted : theme.textPrimary
                                    font.pixelSize: 14
                                    elide: Text.ElideMiddle
                                    Layout.fillWidth: true
                                }
                                Label {
                                    visible: modelData.isDir
                                    text: "›"
                                    color: theme.textDim
                                    font.pixelSize: 16
                                }
                            }
                            MouseArea {
                                id: entryMouse
                                anchors.fill: parent
                                hoverEnabled: true
                                cursorShape: Qt.PointingHandCursor
                                onClicked: {
                                    if (modelData.isDir)
                                        root.navigate(modelData.path)
                                    else
                                        root.selectedFile = modelData.path
                                }
                                onDoubleClicked: {
                                    if (!modelData.isDir) {
                                        root.selectedFile = modelData.path
                                        root.choose()
                                    }
                                }
                            }
                        }
                    }

                    Label {
                        anchors.centerIn: parent
                        width: parent.width - 48
                        horizontalAlignment: Text.AlignHCenter
                        wrapMode: Text.WordWrap
                        visible: root.listing.error.length > 0 || root.visibleEntries.length === 0
                        text: root.listing.error.length > 0 ? root.listing.error
                              : (root.pickFiles ? "This folder is empty." : "No folders inside. You can choose this one or create a new folder.")
                        color: root.listing.error.length > 0 ? theme.dangerStrong : theme.textMuted
                        font.pixelSize: 13
                    }
                }

                // New folder
                RowLayout {
                    visible: root.creating
                    Layout.fillWidth: true
                    Layout.leftMargin: 12
                    Layout.rightMargin: 12
                    Layout.bottomMargin: 8
                    spacing: 8
                    Field {
                        id: newName
                        Layout.fillWidth: true
                        placeholderText: "New folder name"
                        error: root.createError
                        onAccepted: createBtn.clicked()
                    }
                    TealButton {
                        id: createBtn
                        Layout.alignment: Qt.AlignTop
                        text: "Create"
                        compact: true
                        onClicked: {
                            const res = JSON.parse(root.controller.makeFolder(root.listing.path, newName.text))
                            if (res.error.length > 0) {
                                root.createError = res.error
                                return
                            }
                            root.creating = false
                            root.createError = ""
                            newName.text = ""
                            root.navigate(res.path)
                        }
                    }
                    TealButton {
                        Layout.alignment: Qt.AlignTop
                        text: "Cancel"
                        compact: true
                        primary: false
                        onClicked: {
                            root.creating = false
                            root.createError = ""
                        }
                    }
                }
            }
        }

        Rectangle { Layout.fillWidth: true; height: 1; color: theme.borderSubtle }

        RowLayout {
            Layout.fillWidth: true
            Layout.margins: 14
            spacing: 10

            TealButton {
                text: "New Folder"
                primary: false
                compact: true
                visible: !root.pickFiles
                enabled: root.listing.error.length === 0 && !root.creating
                onClicked: {
                    root.creating = true
                    newName.input.forceActiveFocus()
                }
            }
            TealSwitch {
                id: hiddenSwitch
                checked: root.showHidden
                onToggled: root.showHidden = checked
            }
            Label {
                text: "Show hidden"
                color: theme.textMuted
                font.pixelSize: 13
            }
            Item { Layout.fillWidth: true }
            TealButton {
                text: "Cancel"
                primary: false
                onClicked: root.close()
            }
            TealButton {
                text: root.pickFiles ? "Choose" : (root.currentName.length > 0 ? "Choose “" + root.currentName + "”" : "Choose")
                enabled: root.listing.error.length === 0 && (root.pickFiles ? root.selectedFile.length > 0 : root.listing.path.length > 0)
                Layout.maximumWidth: 260
                onClicked: root.choose()
            }
        }
    }
}
