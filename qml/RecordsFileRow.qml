import QtQuick
import QtQuick.Controls
import QtQuick.Layouts
import app.verfsnext 1.0

Rectangle {
    id: root
    readonly property VerfsTheme theme: VerfsTheme {}
    required property var controller
    required property var modelData
    /// Shows the "Move to lost+found" action for files still in their place.
    property bool movable: false
    /// Why the file cannot be moved, shown instead of the action.
    property string blockedReason: ""
    /// How the pieces of this file are broken: "lost", "damaged"…
    property string problemText: "damaged"

    function parentFolder() {
        const path = root.modelData.path
        if (path === null)
            return root.controller.mount_point
        const cut = path.lastIndexOf("/")
        return cut > 0 ? root.controller.mount_point + "/" + path.substring(0, cut) : root.controller.mount_point
    }

    Layout.fillWidth: true
    radius: theme.radiusSm
    color: theme.inputBg
    border.width: 1
    border.color: theme.borderSubtle
    implicitHeight: content.implicitHeight + 20

    ColumnLayout {
        id: content
        anchors.left: parent.left
        anchors.right: parent.right
        anchors.top: parent.top
        anchors.margins: 10
        spacing: 4

        Label {
            Layout.fillWidth: true
            text: root.modelData.path === null
                  ? "File without a folder entry (inode " + root.modelData.ino + ")"
                  : "/" + root.modelData.path
            color: root.modelData.path === null ? theme.textMuted : theme.textPrimary
            font.pixelSize: 13
            font.family: "monospace"
            elide: Text.ElideMiddle
        }

        Label {
            Layout.fillWidth: true
            text: theme.bytes(root.modelData.size_bytes) + " · " + root.modelData.pieces + " "
                  + root.problemText + (root.modelData.pieces === 1 ? " piece" : " pieces")
                  + " · first at byte " + root.modelData.first_offset
            color: theme.textMuted
            font.pixelSize: 12
        }

        Label {
            Layout.fillWidth: true
            visible: root.blockedReason.length > 0
            text: root.blockedReason
            color: theme.textSecondary
            font.pixelSize: 12
            wrapMode: Text.WordWrap
        }

        RowLayout {
            spacing: 8

            TealButton {
                visible: root.modelData.path !== null
                         && !(root.modelData.path.indexOf(".vault/") === 0 && root.blockedReason.length > 0)
                text: "Show in folder"
                primary: false
                compact: true
                onClicked: root.controller.openPath(root.parentFolder())
            }

            TealButton {
                visible: root.movable
                text: "Move to lost+found"
                primary: false
                compact: true
                onClicked: root.controller.moveDamagedFiles(JSON.stringify([root.modelData.path]))
            }
        }
    }
}
