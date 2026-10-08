import QtQuick
import QtQuick.Controls
import app.verfsnext 1.0

Rectangle {
    id: root
    readonly property VerfsTheme theme: VerfsTheme {}
    color: theme.cardBg
    radius: theme.radius
    border.width: 1
    border.color: "#1f242d"

    // Rectangle never derives implicit size from children; forward the
    // body's height so layouts don't collapse the card. The width is fixed:
    // forwarding it would make wrapped text widen every enclosing layout
    // (and trigger polish loops). Cards fill their width in layouts.
    implicitWidth: 320
    implicitHeight: body.implicitHeight + root.contentPadding * 2

    default property alias content: body.data
    property alias title: titleLabel.text
    property alias subtitle: subtitleLabel.text
    property int contentPadding: 20
    property int contentSpacing: 14

    Column {
        id: body
        anchors.left: parent.left
        anchors.right: parent.right
        anchors.top: parent.top
        anchors.margins: root.contentPadding
        spacing: root.contentSpacing

        Column {
            width: parent.width
            spacing: 4
            visible: titleLabel.text.length > 0

            Label {
                id: titleLabel
                color: theme.textPrimary
                font.pixelSize: 16
                font.bold: true
                width: parent.width
                wrapMode: Text.WordWrap
            }
            Label {
                id: subtitleLabel
                visible: text.length > 0
                color: theme.textMuted
                font.pixelSize: 13
                width: parent.width
                wrapMode: Text.WordWrap
            }
        }
    }
}
