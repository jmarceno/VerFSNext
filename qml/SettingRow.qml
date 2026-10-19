import QtQuick
import QtQuick.Controls
import QtQuick.Layouts
import app.verfsnext 1.0

// One config option: label + help on the left, the right control on the right.
RowLayout {
    id: root
    readonly property VerfsTheme theme: VerfsTheme {}
    required property var field
    required property var value
    property bool locked: false
    property bool invalid: false
    signal edited(var value, bool valid)

    readonly property bool isDefault: String(root.value) === String(root.field.defaultValue)

    spacing: 24

    function levelName(v) {
        if (v < 0) return "Fastest"
        if (v <= 3) return "Fast"
        if (v <= 9) return "Balanced"
        if (v <= 15) return "Smaller files"
        return "Smallest files (slow)"
    }

    function shown(v) {
        if (root.field.kind === "bool")
            return v ? "On" : "Off"
        if (root.field.kind === "level")
            return v + " (" + levelName(v) + ")"
        return String(v) + (root.field.unit.length > 0 ? " " + root.field.unit : "")
    }

    ColumnLayout {
        Layout.fillWidth: true
        spacing: 3
        Label {
            text: root.field.label
            color: theme.textPrimary
            font.pixelSize: 14
            Layout.fillWidth: true
            wrapMode: Text.WordWrap
        }
        Label {
            text: root.field.help
            color: theme.textMuted
            font.pixelSize: 12
            Layout.fillWidth: true
            wrapMode: Text.WordWrap
        }
        RowLayout {
            spacing: 8
            visible: !root.isDefault || root.locked
            Label {
                visible: root.locked
                text: "Fixed after setup"
                color: theme.warning
                font.pixelSize: 11
                font.bold: true
            }
            Label {
                visible: !root.isDefault && !root.locked
                text: "Default: " + root.shown(root.field.defaultValue)
                color: theme.textDim
                font.pixelSize: 11
            }
            Label {
                visible: !root.isDefault && !root.locked
                text: "Restore"
                color: theme.accent
                font.pixelSize: 11
                font.bold: true
                MouseArea {
                    anchors.fill: parent
                    anchors.margins: -4
                    cursorShape: Qt.PointingHandCursor
                    onClicked: {
                        root.edited(root.field.defaultValue, true)
                        if (root.field.kind === "number" || root.field.kind === "text")
                            input.text = String(root.field.defaultValue)
                    }
                }
            }
        }
    }

    Item {
        Layout.preferredWidth: 240
        Layout.alignment: Qt.AlignVCenter
        implicitHeight: Math.max(sw.visible ? sw.implicitHeight : 0,
                                 inputBox.visible ? 40 : 0,
                                 levelBox.visible ? levelBox.implicitHeight : 0)

        TealSwitch {
            id: sw
            visible: root.field.kind === "bool"
            enabled: !root.locked
            anchors.right: parent.right
            anchors.verticalCenter: parent.verticalCenter
            checked: root.field.kind === "bool" && root.value === true
            onToggled: root.edited(checked, true)
        }

        Rectangle {
            id: inputBox
            visible: root.field.kind === "number" || root.field.kind === "text"
            anchors.fill: parent
            radius: theme.radiusSm
            color: theme.inputBg
            opacity: root.locked ? 0.6 : 1.0
            border.width: 1
            border.color: root.invalid ? theme.danger : (input.activeFocus ? theme.accent : theme.borderSubtle)

            RowLayout {
                anchors.fill: parent
                anchors.leftMargin: 12
                anchors.rightMargin: 12
                spacing: 6
                TextInput {
                    id: input
                    Layout.fillWidth: true
                    readOnly: root.locked
                    text: String(root.value)
                    color: theme.textPrimary
                    selectionColor: theme.accentMuted
                    font.pixelSize: 14
                    clip: true
                    horizontalAlignment: root.field.kind === "number" ? TextInput.AlignRight : TextInput.AlignLeft
                    selectByMouse: true
                    onTextEdited: {
                        if (root.field.kind === "text") {
                            root.edited(text, text.trim().length > 0)
                            return
                        }
                        const n = Number(text.trim().replace(",", "."))
                        const ok = text.trim().length > 0 && !isNaN(n) && isFinite(n)
                        root.edited(ok ? n : text, ok)
                    }
                }
                Label {
                    visible: root.field.unit.length > 0
                    text: root.field.unit
                    color: theme.textMuted
                    font.pixelSize: 13
                }
            }
        }

        ColumnLayout {
            id: levelBox
            visible: root.field.kind === "level"
            anchors.left: parent.left
            anchors.right: parent.right
            anchors.verticalCenter: parent.verticalCenter
            spacing: 2

            Label {
                Layout.alignment: Qt.AlignRight
                text: root.field.kind === "level" ? root.shown(Math.round(slider.value)) : ""
                color: theme.accentStrong
                font.pixelSize: 13
                font.bold: true
            }
            Slider {
                id: slider
                Layout.fillWidth: true
                Layout.preferredHeight: 24
                from: -7
                to: 22
                stepSize: 1
                snapMode: Slider.SnapAlways
                value: root.field.kind === "level" ? root.value : 0
                onMoved: root.edited(Math.round(value), true)

                background: Rectangle {
                    x: slider.leftPadding
                    y: slider.topPadding + slider.availableHeight / 2 - height / 2
                    implicitWidth: 200
                    implicitHeight: 4
                    width: slider.availableWidth
                    height: 4
                    radius: 2
                    color: theme.sliderTrack
                    Rectangle {
                        width: slider.visualPosition * parent.width
                        height: parent.height
                        radius: 2
                        color: theme.accent
                    }
                }
                handle: Rectangle {
                    x: slider.leftPadding + slider.visualPosition * (slider.availableWidth - width)
                    y: slider.topPadding + slider.availableHeight / 2 - height / 2
                    implicitWidth: 18
                    implicitHeight: 18
                    radius: 9
                    color: "#ffffff"
                }
            }
            RowLayout {
                Layout.fillWidth: true
                Label { text: "Faster"; color: theme.textDim; font.pixelSize: 11 }
                Item { Layout.fillWidth: true }
                Label { text: "Smaller"; color: theme.textDim; font.pixelSize: 11 }
            }
        }
    }
}
