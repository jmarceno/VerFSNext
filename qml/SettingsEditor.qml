import QtQuick
import QtQuick.Controls
import QtQuick.Layouts
import app.verfsnext 1.0

// Grouped editor over the settings model from Rust (settings.rs). Collects
// changes in `edits` ({key: value}); invalid entries block saving.
ColumnLayout {
    id: root
    readonly property VerfsTheme theme: VerfsTheme {}
    property string modelJson: ""
    property bool setupMode: false
    property bool showAdvanced: false
    property var edits: ({})
    property var invalid: ({})
    property int version: 0
    readonly property var fields: modelJson.length > 0 ? JSON.parse(modelJson) : []
    readonly property bool dirty: Object.keys(edits).length > 0
    readonly property bool valid: Object.keys(invalid).length === 0
    readonly property var groups: {
        const seen = []
        for (const f of fields) {
            if ((showAdvanced || !f.advanced) && seen.indexOf(f.group) < 0)
                seen.push(f.group)
        }
        return seen
    }

    function editsJson() {
        return JSON.stringify(edits)
    }

    function reset() {
        edits = {}
        invalid = {}
        version++
    }

    function record(field, value, ok) {
        const e = Object.assign({}, edits)
        const bad = Object.assign({}, invalid)
        if (ok) {
            delete bad[field.key]
            if (String(value) === String(field.value))
                delete e[field.key]
            else
                e[field.key] = value
        } else {
            bad[field.key] = true
            delete e[field.key]
        }
        edits = e
        invalid = bad
    }

    spacing: 16

    onModelJsonChanged: reset()

    Repeater {
        model: root.groups
        delegate: Card {
            id: groupCard
            required property string modelData
            Layout.fillWidth: true
            title: modelData

            Repeater {
                model: {
                    root.version
                    return root.fields.filter(f => f.group === groupCard.modelData && (root.showAdvanced || !f.advanced))
                }
                delegate: Column {
                    required property var modelData
                    required property int index
                    width: parent ? parent.width : 0
                    spacing: 14

                    Rectangle {
                        visible: index > 0
                        width: parent.width
                        height: 1
                        color: theme.borderSubtle
                    }
                    SettingRow {
                        width: parent.width
                        field: modelData
                        value: modelData.key in root.edits ? root.edits[modelData.key] : modelData.value
                        locked: modelData.setupOnly && !root.setupMode
                        invalid: modelData.key in root.invalid
                        onEdited: (value, ok) => root.record(modelData, value, ok)
                    }
                }
            }
        }
    }

    RowLayout {
        Layout.fillWidth: true
        spacing: 12
        TealSwitch {
            checked: root.showAdvanced
            onToggled: root.showAdvanced = checked
        }
        ColumnLayout {
            spacing: 2
            Layout.fillWidth: true
            Label {
                text: "Show advanced options"
                color: theme.textPrimary
                font.pixelSize: 14
            }
            Label {
                text: "Fine-tuning for experienced users. The defaults work well for most people."
                color: theme.textMuted
                font.pixelSize: 12
                Layout.fillWidth: true
                wrapMode: Text.WordWrap
            }
        }
    }
}
