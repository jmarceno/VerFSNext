import QtQuick
import QtQuick.Controls
import QtQuick.Layouts
import app.verfsnext 1.0

Popup {
    id: root
    readonly property VerfsTheme theme: VerfsTheme {}
    required property var controller

    readonly property var report: controller.records_json.length > 0 ? JSON.parse(controller.records_json) : null
    readonly property var status: controller.status_json.length > 0 ? JSON.parse(controller.status_json) : null
    readonly property bool vaultLocked: status !== null && status.vault_locked
    readonly property var missingFiles: report ? report.files_with_missing_pieces : []
    readonly property var damagedFiles: report ? report.files_with_damaged_copies : []
    readonly property var driftedFiles: report ? report.files_with_refcount_drift : []
    /// Files that lost data and still sit in their own folder.
    readonly property var awaitingFiles: missingFiles.filter(function (f) { return !root.isParked(f.path) })
    /// Of those, the ones a rename can actually move right now.
    readonly property var movableFiles: awaitingFiles.filter(function (f) { return root.blockedReason(f.path) === "" })
    /// Files that lost data and already wait in lost+found for deletion.
    readonly property var parkedFiles: missingFiles.filter(function (f) { return root.isParked(f.path) })
    /// Of those, the ones that can be deleted right now.
    readonly property var deletableFiles: parkedFiles.filter(function (f) { return root.blockedReason(f.path) === "" })

    /// Vault files live behind the vault boundary, which a rename cannot
    /// cross, so they get their own lost+found inside the vault.
    function lostFoundRoot(path) {
        return path !== null && path.indexOf(".vault/") === 0 ? ".vault/lost+found" : "lost+found"
    }
    function isParked(path) {
        return path !== null && path.indexOf(root.lostFoundRoot(path) + "/") === 0
    }
    /// Why a file cannot be moved to lost+found right now, or "" when it can.
    function blockedReason(path) {
        if (path === null)
            return "No folder links to this file any more, so there is nothing to move."
        if (path.indexOf(".snapshots/") === 0)
            return "This copy belongs to a read-only snapshot. Delete the snapshot to release it."
        if (path.indexOf(".vault/") === 0 && root.vaultLocked)
            return "This file is in the vault. Unlock the vault to move it."
        return ""
    }

    readonly property int missingCount: report ? report.missing_chunk_records_for_extents : 0
    readonly property int driftedCount: report ? report.chunk_refcount_mismatch_count : 0
    readonly property int orphanCount: report ? report.orphan_extent_records : 0
    readonly property int crcCount: report ? report.pack_crc32_read_error_count : 0
    readonly property bool truncated: report ? report.list_truncated : false
    readonly property bool clean: report !== null && missingCount === 0 && driftedCount === 0
                                 && orphanCount === 0 && damagedFiles.length === 0

    function moveAllMovable() {
        root.controller.moveDamagedFiles(JSON.stringify(movableFiles.map(function (f) { return f.path })))
    }

    function deleteAllParked() {
        root.controller.deleteLostFoundFiles(JSON.stringify(deletableFiles.map(function (f) { return f.path })))
    }

    modal: true
    focus: true
    anchors.centerIn: Overlay.overlay
    width: Math.min(760, Overlay.overlay ? Overlay.overlay.width - 64 : 760)
    height: Math.min(700, Overlay.overlay ? Overlay.overlay.height - 64 : 700)
    padding: 24
    closePolicy: Popup.CloseOnEscape | Popup.CloseOnPressOutside

    /// Result of the last move or delete, shown inside the dialog because
    /// the window toast sits under this modal popup.
    property string feedback: ""
    property bool feedbackError: false

    Connections {
        target: root.controller
        enabled: root.opened
        function onToast_serialChanged() {
            root.feedback = root.controller.toast
            root.feedbackError = root.controller.toast_error
        }
    }

    onOpened: root.controller.checkRecords()
    onClosed: {
        root.feedback = ""
        moveConfirm.close()
        deleteConfirm.close()
    }

    Overlay.modal: Rectangle { color: "#99000000" }

    background: Rectangle {
        radius: theme.radius
        color: theme.cardBgRaised
        border.width: 1
        border.color: theme.borderSubtle
    }

    ConfirmPopup {
        id: moveConfirm
        readonly property int count: root.movableFiles.length
        title: count === 1 ? "Move 1 damaged file to lost+found?" : "Move " + count + " damaged files to lost+found?"
        message: "The files keep their place in the tree inside lost+found/ (vault files go to .vault/lost+found/). Nothing is deleted — you can open the folder and delete them there when you no longer need them."
        confirmText: "Move"
        onConfirmed: root.moveAllMovable()
    }

    ConfirmPopup {
        id: deleteConfirm
        readonly property int count: root.deletableFiles.length
        title: count === 1 ? "Delete 1 damaged file?" : "Delete " + count + " damaged files?"
        message: "These files are already unreadable and cannot be recovered by VerFSNext. Deleting them releases the records they left behind. This cannot be undone."
        confirmText: "Delete permanently"
        danger: true
        onConfirmed: root.deleteAllParked()
    }

    contentItem: ColumnLayout {
        spacing: 12

        RowLayout {
            Layout.fillWidth: true
            spacing: 12

            ColumnLayout {
                Layout.fillWidth: true
                spacing: 2
                Label {
                    text: "Records & data health"
                    color: theme.textPrimary
                    font.pixelSize: 18
                    font.bold: true
                }
                Label {
                    Layout.fillWidth: true
                    text: "VerFSNext checks every record it keeps about your files and lists what needs attention."
                    color: theme.textMuted
                    font.pixelSize: 12
                    wrapMode: Text.WordWrap
                }
            }

            Spinner { visible: root.controller.records_loading }
        }

        ScrollView {
            id: scroll
            Layout.fillWidth: true
            Layout.fillHeight: true
            clip: true
            contentWidth: availableWidth
            ScrollBar.horizontal.policy: ScrollBar.AlwaysOff

            ColumnLayout {
                width: scroll.availableWidth
                spacing: 10

                Label {
                    Layout.fillWidth: true
                    visible: root.controller.records_error.length > 0
                    text: "The check could not run: " + root.controller.records_error
                    color: theme.danger
                    font.pixelSize: 13
                    wrapMode: Text.WordWrap
                }

                Label {
                    Layout.fillWidth: true
                    visible: root.report === null && root.controller.records_error.length === 0
                    text: root.controller.records_loading ? "Checking every file…" : "No check has run yet."
                    color: theme.textMuted
                    font.pixelSize: 13
                }

                Label {
                    Layout.fillWidth: true
                    visible: root.clean
                    text: "Everything is in order. No lost pieces, no damaged copies, no bookkeeping problems."
                    color: theme.statusGreen
                    font.pixelSize: 13
                    wrapMode: Text.WordWrap
                }

                GridLayout {
                    Layout.fillWidth: true
                    visible: root.report !== null
                    columns: 2
                    columnSpacing: 24
                    rowSpacing: 6

                    Label { text: "Lost data pieces"; color: theme.textMuted; font.pixelSize: 13 }
                    Label {
                        text: root.missingCount
                        color: root.missingCount > 0 ? theme.warning : theme.textPrimary
                        font.pixelSize: 13
                        font.bold: root.missingCount > 0
                    }
                    Label { text: "Damaged stored copies"; color: theme.textMuted; font.pixelSize: 13 }
                    Label {
                        text: root.damagedFiles.length
                        color: root.damagedFiles.length > 0 ? theme.warning : theme.textPrimary
                        font.pixelSize: 13
                        font.bold: root.damagedFiles.length > 0
                    }
                    Label { text: "Wrong reference counts"; color: theme.textMuted; font.pixelSize: 13 }
                    Label {
                        text: root.driftedCount
                        color: root.driftedCount > 0 ? theme.warning : theme.textPrimary
                        font.pixelSize: 13
                        font.bold: root.driftedCount > 0
                    }
                    Label { text: "Leftover records"; color: theme.textMuted; font.pixelSize: 13 }
                    Label { text: root.orphanCount; color: theme.textPrimary; font.pixelSize: 13 }
                    Label { text: "Checksum errors since start"; color: theme.textMuted; font.pixelSize: 13 }
                    Label { text: root.crcCount; color: theme.textPrimary; font.pixelSize: 13 }
                }

                // Files that lost data and are still in their own folder.
                Label {
                    Layout.fillWidth: true
                    Layout.topMargin: 6
                    visible: root.awaitingFiles.length > 0
                    text: root.awaitingFiles.length === 1
                          ? "1 file lost data"
                          : root.awaitingFiles.length + " files lost data"
                    color: theme.warning
                    font.pixelSize: 15
                    font.bold: true
                }
                Label {
                    Layout.fillWidth: true
                    visible: root.awaitingFiles.length > 0
                    text: "The parts listed below were lost — usually after a power loss or a crash that damaged the records. "
                          + "The rest of each file is intact, but reading the lost part fails. VerFSNext cannot bring this data back. "
                          + "Copy the file from a backup or another computer to repair it, or move it out of the way and delete it later."
                    color: theme.textSecondary
                    font.pixelSize: 13
                    wrapMode: Text.WordWrap
                }
                Repeater {
                    model: root.awaitingFiles
                    delegate: RecordsFileRow {
                        controller: root.controller
                        blockedReason: root.blockedReason(modelData.path)
                        movable: blockedReason === ""
                        problemText: "lost"
                    }
                }
                TealButton {
                    Layout.topMargin: 2
                    visible: root.movableFiles.length > 1
                    text: "Move all " + root.movableFiles.length + " files to lost+found"
                    primary: false
                    onClicked: moveConfirm.open()
                }

                // Files that already wait in lost+found.
                Label {
                    Layout.fillWidth: true
                    Layout.topMargin: 6
                    visible: root.parkedFiles.length > 0
                    text: root.parkedFiles.length === 1
                          ? "1 damaged file waits in lost+found"
                          : root.parkedFiles.length + " damaged files wait in lost+found"
                    color: theme.textSecondary
                    font.pixelSize: 15
                    font.bold: true
                }
                Label {
                    Layout.fillWidth: true
                    visible: root.parkedFiles.length > 0
                    text: "These are the files you moved out of the way. They still count as damaged records until you delete them."
                    color: theme.textSecondary
                    font.pixelSize: 13
                    wrapMode: Text.WordWrap
                }
                Repeater {
                    model: root.parkedFiles
                    delegate: RecordsFileRow {
                        controller: root.controller
                        blockedReason: root.blockedReason(modelData.path)
                        problemText: "lost"
                    }
                }
                RowLayout {
                    Layout.topMargin: 2
                    visible: root.parkedFiles.length > 0
                    spacing: 8
                    TealButton {
                        text: "Open lost+found folder"
                        primary: false
                        onClicked: root.controller.openPath(root.controller.mount_point + "/"
                                                            + root.lostFoundRoot(root.parkedFiles[0].path))
                    }
                    TealButton {
                        visible: root.deletableFiles.length > 0
                        text: root.deletableFiles.length === 1 ? "Delete this file" : "Delete these " + root.deletableFiles.length + " files"
                        danger: true
                        onClicked: deleteConfirm.open()
                    }
                }

                // Files whose stored copies are damaged.
                Label {
                    Layout.fillWidth: true
                    Layout.topMargin: 6
                    visible: root.damagedFiles.length > 0
                    text: root.damagedFiles.length === 1
                          ? "1 file has a damaged stored copy"
                          : root.damagedFiles.length + " files have damaged stored copies"
                    color: theme.warning
                    font.pixelSize: 15
                    font.bold: true
                }
                Label {
                    Layout.fillWidth: true
                    visible: root.damagedFiles.length > 0
                    text: "A stored copy of some of their data failed its checksum while being read, so reading the file fails. "
                          + "Writing the content again — for example copying the file from a backup — stores a fresh copy."
                    color: theme.textSecondary
                    font.pixelSize: 13
                    wrapMode: Text.WordWrap
                }
                Repeater {
                    model: root.damagedFiles
                    delegate: RecordsFileRow {
                        controller: root.controller
                        movable: true
                    }
                }

                // Reference-count drift: bookkeeping only.
                Label {
                    Layout.fillWidth: true
                    Layout.topMargin: 6
                    visible: root.driftedFiles.length > 0
                    text: root.driftedFiles.length === 1
                          ? "1 file points at a piece with a wrong reference count"
                          : root.driftedFiles.length + " files point at pieces with wrong reference counts"
                    color: theme.textSecondary
                    font.pixelSize: 15
                    font.bold: true
                }
                Label {
                    Layout.fillWidth: true
                    visible: root.driftedFiles.length > 0
                    text: "This is bookkeeping only: the file data is fine and reading these files works. "
                          + "VerFSNext keeps the pieces safe and nothing needs to be done here."
                    color: theme.textSecondary
                    font.pixelSize: 13
                    wrapMode: Text.WordWrap
                }
                Repeater {
                    model: root.driftedFiles
                    delegate: RecordsFileRow {
                        controller: root.controller
                        problemText: "affected"
                    }
                }

                // Orphan extent records: leftover bookkeeping.
                Label {
                    Layout.fillWidth: true
                    Layout.topMargin: 6
                    visible: root.orphanCount > 0
                    text: root.orphanCount === 1 ? "1 leftover record" : root.orphanCount + " leftover records"
                    color: theme.textSecondary
                    font.pixelSize: 15
                    font.bold: true
                }
                Label {
                    Layout.fillWidth: true
                    visible: root.orphanCount > 0
                    text: "Old bookkeeping for files that no longer exist. It costs a few bytes and does not affect your files."
                    color: theme.textSecondary
                    font.pixelSize: 13
                    wrapMode: Text.WordWrap
                }

                Label {
                    Layout.fillWidth: true
                    visible: root.crcCount > 0 && root.damagedFiles.length === 0
                    text: "Copies that failed their checksum while being read. Files show up in the list above once the read that found them completes."
                    color: theme.textSecondary
                    font.pixelSize: 13
                    wrapMode: Text.WordWrap
                }

                Label {
                    Layout.fillWidth: true
                    visible: root.truncated
                    text: "Only the first files of each list are shown."
                    color: theme.textMuted
                    font.pixelSize: 12
                }
            }
        }

        RowLayout {
            Layout.fillWidth: true
            spacing: 8

            TealButton {
                text: "Re-check"
                primary: false
                enabled: !root.controller.records_loading
                onClicked: root.controller.checkRecords()
            }
            Spinner { visible: root.controller.busy }
            Label {
                Layout.fillWidth: true
                text: root.controller.busy ? root.controller.busy_text : root.feedback
                color: !root.controller.busy && root.feedbackError ? theme.danger : theme.textSecondary
                font.pixelSize: 12
                wrapMode: Text.WordWrap
            }
            TealButton {
                text: "Close"
                primary: false
                onClicked: root.close()
            }
        }
    }
}
