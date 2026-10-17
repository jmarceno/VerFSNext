import QtQuick
import QtQuick.Controls
import QtQuick.Layouts
import app.verfsnext 1.0

ScrollView {
    id: root
    readonly property VerfsTheme theme: VerfsTheme {}
    required property var controller
    readonly property string daemonState: controller.daemon_state
    readonly property bool running: daemonState === "running"
    readonly property var stats: controller.stats_json.length > 0 ? JSON.parse(controller.stats_json) : null
    readonly property var status: controller.status_json.length > 0 ? JSON.parse(controller.status_json) : null
    property bool showAll: false
    /// The daemon keeps the last activity — the speeds and the memory and
    /// cache readings taken with them — so the Memory card still shows numbers
    /// after a transfer ends, even if this window was closed while it ran.
    readonly property var lastActivity: status ? status.last_activity : null
    /// True while the daemon's latest one-second sample moved data.
    readonly property bool moving: status !== null && (status.read_throughput_bps > 0 || status.write_throughput_bps > 0)
    /// The daemon's kept readings, all zero until its first activity.
    readonly property real keptReadBps: lastActivity ? lastActivity.read_bps : 0
    readonly property real keptWriteBps: lastActivity ? lastActivity.write_bps : 0
    readonly property real keptCacheHitRate: lastActivity ? lastActivity.cache_hit_rate : 0
    readonly property real keptCacheMemoryBytes: lastActivity ? lastActivity.cache_memory_bytes : 0
    readonly property real keptRssBytes: lastActivity ? lastActivity.process_rss_bytes : 0
    readonly property string lastActivityAgo: lastActivity ? theme.duration(lastActivity.ago_secs) + " ago" : ""
    /// True while the Memory card shows the readings kept from the last
    /// activity instead of live ones.
    readonly property bool showingLastActivity: !moving && lastActivity !== null
    /// Speeds for the statistics list: live while data moves, otherwise the
    /// rates kept from the last activity.
    readonly property real shownReadBps: moving ? status.read_throughput_bps : keptReadBps
    readonly property real shownWriteBps: moving ? status.write_throughput_bps : keptWriteBps

    readonly property real logicalAll: stats ? stats.all_logical_size_bytes : 0
    readonly property real onDisk: stats ? stats.data_dir_size_bytes : 0
    readonly property real saved: stats ? stats.net_saved_bytes : 0
    readonly property real compressionSaving: stats ? stats.compression_savings_ratio : 0
    readonly property real dedupSaving: stats ? stats.dedup_savings_ratio : 0
    readonly property bool healthy: stats ? (stats.chunk_refcount_mismatch_count === 0
                                             && stats.missing_chunk_records_for_extents === 0
                                             && stats.orphan_extent_records === 0) : true

    signal snapshotRequested()

    contentWidth: availableWidth
    clip: true

    function allRows() {
        if (!stats)
            return []
        const b = theme.bytes
        return [
            ["Your files (live folder)", b(stats.live_logical_size_bytes)],
            ["Files kept by snapshots", b(stats.snapshots_logical_size_bytes)],
            ["Hidden in locked vault", b(stats.hidden_vault_logical_size_bytes)],
            ["Everything reachable", b(stats.all_logical_size_bytes)],
            ["Referenced data (excluding holes)", b(stats.referenced_data_bytes)],
            ["Dedup savings (all namespaces)", b(stats.dedup_saved_bytes) + " (" + theme.percent(stats.dedup_savings_ratio) + ")"],
            ["Unique data before compression", b(stats.stored_unique_uncompressed_bytes)],
            ["Unique data after compression", b(stats.stored_unique_compressed_bytes)],
            ["Compression savings", b(stats.compression_saved_bytes) + " (" + theme.percent(stats.compression_savings_ratio) + ")"],
            ["Unreachable indexed payload (partial garbage)", b(stats.unreachable_chunk_compressed_bytes)],
            ["Obsolete data awaiting cleanup", b(stats.obsolete_awaiting_cleanup_bytes)],
            ["Net savings (all namespaces)", b(stats.net_saved_bytes) + " (" + theme.percent(stats.net_savings_ratio) + ")"],
            ["Metadata on disk", b(stats.metadata_size_bytes)],
            ["Data folder on disk", b(stats.data_dir_size_bytes)],
            ["Vault", stats.vault_locked ? "Locked" : "Unlocked or not created"],
            ["Reference count mismatches", String(stats.chunk_refcount_mismatch_count)],
            ["Missing piece records", String(stats.missing_chunk_records_for_extents)],
            ["Orphaned file pieces", String(stats.orphan_extent_records)],
            ["Checksum errors while reading", String(stats.pack_crc32_read_error_count)],
            ["Cache hits / requests", stats.cache_hits + " / " + stats.cache_requests],
            ["Cache hit rate", theme.percent(stats.cache_hit_rate)],
            ["Process private memory", b(stats.process_private_memory_bytes)],
            ["Process resident memory (RSS)", b(stats.process_rss_bytes)],
            ["Approximate cache memory", b(stats.approx_cache_memory_bytes)],
            ["Cached file records", String(stats.metadata_cache_entries)],
            ["Cached data pieces", String(stats.chunk_cache_entries)],
            ["Read since start", b(stats.read_bytes_total)],
            ["Written since start", b(stats.write_bytes_total)],
            ["Last activity", root.lastActivity ? root.lastActivityAgo : "none since VerFSNext started"],
            ["Recent read speed", status ? b(root.shownReadBps) + "/s" : "—"],
            ["Recent write speed", status ? b(root.shownWriteBps) + "/s" : "—"],
            ["Throughput sample", status ? status.throughput_sample_secs.toFixed(3) + " s (every second)" : "—"],
            ["Running for", theme.duration(stats.uptime_secs)],
        ]
    }

    ColumnLayout {
        width: root.availableWidth
        spacing: 16

        // Status hero
        Rectangle {
            Layout.fillWidth: true
            radius: theme.radius
            color: theme.cardBg
            border.width: 1
            border.color: root.daemonState === "failed" ? theme.danger : "#1f242d"
            implicitHeight: hero.implicitHeight + 40

            ColumnLayout {
                id: hero
                anchors.left: parent.left
                anchors.right: parent.right
                anchors.top: parent.top
                anchors.margins: 20
                spacing: 14

                RowLayout {
                    Layout.fillWidth: true
                    spacing: 18

                    Item {
                        Layout.preferredWidth: 56
                        Layout.preferredHeight: 56
                        Image {
                            anchors.fill: parent
                            source: theme.iconSource
                            sourceSize.width: 112
                            sourceSize.height: 112
                            opacity: root.running ? 1.0 : 0.45
                        }
                        Spinner {
                            anchors.right: parent.right
                            anchors.bottom: parent.bottom
                            visible: root.daemonState === "starting" || root.daemonState === "stopping"
                        }
                    }

                    ColumnLayout {
                        Layout.fillWidth: true
                        spacing: 4
                        Label {
                            text: {
                                switch (root.daemonState) {
                                case "running": return "Your folder is ready"
                                case "starting": return "Starting VerFSNext…"
                                case "stopping": return "Stopping VerFSNext…"
                                case "failed": return "VerFSNext stopped unexpectedly"
                                default: return "VerFSNext is stopped"
                                }
                            }
                            color: theme.textPrimary
                            font.pixelSize: 20
                            font.bold: true
                            Layout.fillWidth: true
                            wrapMode: Text.WordWrap
                        }
                        Label {
                            text: {
                                switch (root.daemonState) {
                                case "running": return root.controller.mount_point
                                case "starting": return "Getting your folder ready. This usually takes a few seconds."
                                case "stopping": return "Saving everything safely to disk before disconnecting."
                                case "failed": return "Your data is safe on disk. See the details below, then try again."
                                default: return "Start it to use your folder at " + root.controller.mount_point
                                }
                            }
                            color: theme.textMuted
                            font.pixelSize: 13
                            Layout.fillWidth: true
                            wrapMode: Text.WordWrap
                        }
                    }

                    RowLayout {
                        spacing: 8
                        TealButton {
                            visible: root.running
                            text: "Open Folder"
                            onClicked: root.controller.openMountFolder()
                        }
                        TealButton {
                            visible: root.running
                            text: "Take Snapshot"
                            primary: false
                            enabled: !root.controller.busy
                            onClicked: root.snapshotRequested()
                        }
                        TealButton {
                            visible: root.daemonState === "stopped" || root.daemonState === "failed"
                            text: root.daemonState === "failed" ? "Try Again" : "Start VerFSNext"
                            enabled: !root.controller.busy
                            onClicked: root.controller.startDaemon()
                        }
                    }
                }

                Rectangle {
                    visible: root.daemonState === "failed" && root.controller.state_detail.length > 0
                    Layout.fillWidth: true
                    Layout.preferredHeight: Math.min(200, detail.implicitHeight + 24)
                    radius: theme.radiusSm
                    color: theme.inputBg
                    ScrollView {
                        anchors.fill: parent
                        anchors.margins: 12
                        clip: true
                        TextArea {
                            id: detail
                            readOnly: true
                            text: root.controller.state_detail
                            color: theme.textSecondary
                            font.family: "monospace"
                            font.pixelSize: 12
                            wrapMode: Text.WrapAnywhere
                            selectByMouse: true
                            background: null
                            padding: 0
                        }
                    }
                }
            }
        }

        // Space savings
        Card {
            visible: root.running
            Layout.fillWidth: true

            RowLayout {
                width: parent.width
                spacing: 12
                ColumnLayout {
                    Layout.fillWidth: true
                    spacing: 4
                    Label {
                        text: !root.stats ? (root.controller.stats_loading ? "Measuring your space…" : "Space savings")
                              : (root.logicalAll === 0 ? "Nothing stored yet"
                                 : (root.saved > 0 ? "You're saving " + theme.bytes(root.saved)
                                    : "Using " + theme.bytes(-root.saved) + " beyond logical data"))
                        color: theme.textPrimary
                        font.pixelSize: 22
                        font.bold: true
                        Layout.fillWidth: true
                        wrapMode: Text.WordWrap
                    }
                    Label {
                        text: !root.stats ? (root.controller.stats_error.length > 0 ? root.controller.stats_error
                                             : "This looks through all your files and can take a moment on large folders.")
                              : (root.logicalAll === 0 ? "Copy files into your folder and VerFSNext will start removing duplicates and compressing them."
                                 : (root.saved > 0 ? theme.percent(root.stats.net_savings_ratio) + " less space than storing your files and snapshots as they are."
                                    : (root.stats.obsolete_awaiting_cleanup_bytes > 0
                                       ? "Records use " + theme.bytes(root.stats.metadata_size_bytes) + ". Deleted data still on disk uses " + theme.bytes(root.stats.obsolete_awaiting_cleanup_bytes) + " and is reclaimed when the folder is idle."
                                       : "Records use " + theme.bytes(root.stats.metadata_size_bytes) + ". The remaining difference is indexes and pack framing.")))
                        color: root.controller.stats_error.length > 0 && !root.stats ? theme.dangerStrong : theme.textMuted
                        font.pixelSize: 13
                        Layout.fillWidth: true
                        wrapMode: Text.WordWrap
                    }
                }
                ColumnLayout {
                    Layout.alignment: Qt.AlignTop
                    spacing: 6
                    RowLayout {
                        Layout.alignment: Qt.AlignRight
                        spacing: 8
                        Spinner { visible: root.controller.stats_loading }
                        TealButton {
                            text: "Refresh"
                            compact: true
                            primary: false
                            enabled: !root.controller.stats_loading
                            onClicked: root.controller.refreshStats()
                        }
                    }
                    Label {
                        Layout.alignment: Qt.AlignRight
                        visible: root.stats !== null
                        text: {
                            if (root.controller.stats_error.length > 0)
                                return root.controller.stats_error
                            if (!root.stats)
                                return ""
                            const at = "Updated " + Qt.formatTime(new Date(root.stats.updatedAtMs), "hh:mm:ss")
                            return root.controller.stats_refreshing ? at + " · measuring again…" : at
                        }
                        color: root.controller.stats_error.length > 0 ? theme.dangerStrong : theme.textDim
                        font.pixelSize: 11
                    }
                }
            }

            // Disk use vs. everything stored
            Column {
                visible: root.stats !== null && root.logicalAll > 0
                width: parent.width
                spacing: 8
                Rectangle {
                    width: parent.width
                    height: 12
                    radius: 6
                    color: theme.sliderTrack
                    Rectangle {
                        width: parent.width * Math.min(1, root.logicalAll > 0 ? root.onDisk / root.logicalAll : 0)
                        height: parent.height
                        radius: 6
                        color: theme.accent
                    }
                }
                RowLayout {
                    width: parent.width
                    Label {
                        text: "On disk: " + theme.bytes(root.onDisk)
                        color: theme.accentStrong
                        font.pixelSize: 12
                        font.bold: true
                    }
                    Item { Layout.fillWidth: true }
                    Label {
                        text: "Files, snapshots and vault: " + theme.bytes(root.logicalAll)
                        color: theme.textMuted
                        font.pixelSize: 12
                    }
                }
            }
        }

        GridLayout {
            visible: root.running && root.stats !== null
            Layout.fillWidth: true
            columns: root.availableWidth > 760 ? 3 : 2
            columnSpacing: 12
            rowSpacing: 12

            StatTile {
                Layout.fillWidth: true
                Layout.fillHeight: true
                label: "Your files"
                value: root.stats ? theme.bytes(root.stats.live_logical_size_bytes) : "—"
                caption: root.stats && root.stats.vault_locked && root.stats.hidden_vault_logical_size_bytes > 0
                         ? "Plus " + theme.bytes(root.stats.hidden_vault_logical_size_bytes) + " in the locked vault"
                         : "What you see in the folder"
            }
            StatTile {
                Layout.fillWidth: true
                Layout.fillHeight: true
                label: "Kept by snapshots"
                value: root.stats ? theme.bytes(root.stats.snapshots_logical_size_bytes) : "—"
                caption: "Shared with your files when unchanged"
            }
            StatTile {
                Layout.fillWidth: true
                Layout.fillHeight: true
                label: "Used on disk"
                value: root.stats ? theme.bytes(root.stats.data_dir_size_bytes) : "—"
                caption: root.stats
                         ? "Records " + theme.bytes(root.stats.metadata_size_bytes)
                           + (root.stats.obsolete_awaiting_cleanup_bytes > 0
                              ? "; " + theme.bytes(root.stats.obsolete_awaiting_cleanup_bytes) + " awaiting cleanup"
                              : "")
                         : ""
            }
            StatTile {
                Layout.fillWidth: true
                Layout.fillHeight: true
                label: "Duplicates removed"
                value: theme.percent(root.dedupSaving)
                valueColor: theme.accentStrong
                caption: root.stats ? theme.bytes(root.stats.dedup_saved_bytes) + " saved across files, snapshots and vault" : ""
            }
            StatTile {
                Layout.fillWidth: true
                Layout.fillHeight: true
                label: "Compression"
                value: theme.percent(root.compressionSaving)
                valueColor: theme.accentStrong
                caption: root.stats ? theme.bytes(root.stats.compression_saved_bytes) + " saved on reachable unique data" : ""
            }
            StatTile {
                Layout.fillWidth: true
                Layout.fillHeight: true
                label: "Health"
                interactive: root.stats !== null
                value: root.healthy && root.stats && root.stats.pack_crc32_read_error_count === 0 ? "All good" : "Check details"
                valueColor: root.healthy && root.stats && root.stats.pack_crc32_read_error_count === 0 ? theme.statusGreen : theme.warning
                caption: {
                    if (!root.stats)
                        return ""
                    const s = root.stats
                    if (s.pack_crc32_read_error_count > 0)
                        return s.pack_crc32_read_error_count + " damaged copies found while reading — click for details"
                    if (s.missing_chunk_records_for_extents > 0)
                        return s.missing_chunk_records_for_extents + " lost data pieces — click to see the files"
                    if (s.chunk_refcount_mismatch_count > 0)
                        return s.chunk_refcount_mismatch_count + " records with wrong counts — click for details"
                    if (s.orphan_extent_records > 0)
                        return s.orphan_extent_records + " leftover records — click for details"
                    return "Records are consistent — click for details"
                }
                onClicked: recordsReport.open()
            }
        }

        RowLayout {
            visible: root.running
            Layout.fillWidth: true
            spacing: 12

            Card {
                Layout.fillWidth: true
                Layout.alignment: Qt.AlignTop
                title: "Activity"
                subtitle: root.status ? "Since VerFSNext started " + theme.duration(root.status.uptime_secs) + " ago" : ""

                GridLayout {
                    width: parent.width
                    columns: 2
                    rowSpacing: 8
                    Label { text: "Read"; color: theme.textMuted; font.pixelSize: 13 }
                    Label {
                        Layout.alignment: Qt.AlignRight
                        text: root.status ? theme.bytes(root.status.read_bytes_total) : "—"
                        color: theme.textPrimary; font.pixelSize: 13; font.bold: true
                    }
                    Label { text: "Written"; color: theme.textMuted; font.pixelSize: 13 }
                    Label {
                        Layout.alignment: Qt.AlignRight
                        text: root.status ? theme.bytes(root.status.write_bytes_total) : "—"
                        color: theme.textPrimary; font.pixelSize: 13; font.bold: true
                    }
                    Label { text: "Cleaning up"; color: theme.textMuted; font.pixelSize: 13 }
                    Label {
                        Layout.alignment: Qt.AlignRight
                        text: root.status ? (root.status.gc_in_progress ? "Reclaiming space now" : "Idle") : "—"
                        color: root.status && root.status.gc_in_progress ? theme.accentStrong : theme.textPrimary
                        font.pixelSize: 13; font.bold: true
                    }
                }
            }

            Card {
                Layout.fillWidth: true
                Layout.alignment: Qt.AlignTop
                title: "Memory"
                subtitle: {
                    if (!root.status)
                        return "Waiting for the daemon"
                    if (root.showingLastActivity)
                        return "Cache hit rate " + theme.percent(root.keptCacheHitRate)
                               + " — kept from the last activity " + root.lastActivityAgo
                    return "Cache hit rate " + theme.percent(root.status.cache_hit_rate)
                }

                GridLayout {
                    width: parent.width
                    columns: 2
                    rowSpacing: 8
                    Label { text: "In use"; color: theme.textMuted; font.pixelSize: 13 }
                    Label {
                        Layout.alignment: Qt.AlignRight
                        text: root.status ? theme.bytes(root.showingLastActivity ? root.keptRssBytes
                                                                                  : root.status.process_rss_bytes) : "—"
                        color: theme.textPrimary; font.pixelSize: 13; font.bold: true
                    }
                    Label { text: "Cache"; color: theme.textMuted; font.pixelSize: 13 }
                    Label {
                        Layout.alignment: Qt.AlignRight
                        text: root.status ? theme.bytes(root.showingLastActivity ? root.keptCacheMemoryBytes
                                                                                  : root.status.approx_cache_memory_bytes) : "—"
                        color: theme.textPrimary; font.pixelSize: 13; font.bold: true
                    }
                    Label { text: "Speed"; color: theme.textMuted; font.pixelSize: 13 }
                    Label {
                        Layout.alignment: Qt.AlignRight
                        text: root.status ? "↓ " + theme.bytes(root.shownReadBps) + "/s  ↑ "
                                            + theme.bytes(root.shownWriteBps) + "/s" : "—"
                        color: theme.textPrimary; font.pixelSize: 13; font.bold: true
                    }
                }
            }
        }

        // Every statistic
        Card {
            visible: root.running && root.stats !== null
            Layout.fillWidth: true

            RowLayout {
                width: parent.width
                Label {
                    text: "All statistics"
                    color: theme.textPrimary
                    font.pixelSize: 16
                    font.bold: true
                    Layout.fillWidth: true
                }
                Label {
                    text: root.showAll ? "Hide" : "Show"
                    color: theme.accent
                    font.pixelSize: 13
                    font.bold: true
                    MouseArea {
                        anchors.fill: parent
                        anchors.margins: -6
                        cursorShape: Qt.PointingHandCursor
                        onClicked: root.showAll = !root.showAll
                    }
                }
            }

            GridLayout {
                visible: root.showAll
                width: parent.width
                columns: 2
                rowSpacing: 6
                columnSpacing: 16
                Repeater {
                    model: root.showAll ? root.allRows() : []
                    delegate: Label {
                        required property var modelData
                        required property int index
                        text: modelData[0]
                        color: theme.textMuted
                        font.pixelSize: 13
                        Layout.row: index
                        Layout.column: 0
                        Layout.fillWidth: true
                    }
                }
                Repeater {
                    model: root.showAll ? root.allRows() : []
                    delegate: Label {
                        required property var modelData
                        required property int index
                        text: modelData[1]
                        color: theme.textPrimary
                        font.pixelSize: 13
                        Layout.row: index
                        Layout.column: 1
                        Layout.alignment: Qt.AlignRight
                    }
                }
            }
        }

        Item { Layout.preferredHeight: 8 }

        RecordsReport {
            id: recordsReport
            controller: root.controller
        }
    }
}
