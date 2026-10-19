import QtQuick
import QtQuick.Controls
import app.verfsnext 1.0

Button {
    id: root
    readonly property VerfsTheme theme: VerfsTheme {}
    property bool primary: true
    property bool danger: false
    property bool compact: false

    leftPadding: compact ? 12 : 18
    rightPadding: compact ? 12 : 18
    topPadding: compact ? 7 : 10
    bottomPadding: compact ? 7 : 10
    font.pixelSize: compact ? 13 : 14
    font.bold: true
    hoverEnabled: true

    contentItem: Label {
        text: root.text
        color: root.primary && !root.danger ? theme.accentInk : (root.danger ? theme.dangerStrong : theme.textPrimary)
        font: root.font
        opacity: root.enabled ? 1.0 : 0.55
        horizontalAlignment: Text.AlignHCenter
        verticalAlignment: Text.AlignVCenter
        elide: Text.ElideRight
    }

    background: Rectangle {
        implicitHeight: root.compact ? 32 : 40
        implicitWidth: 64
        radius: theme.radiusSm
        opacity: root.enabled ? 1.0 : 0.55
        color: {
            if (root.danger)
                return root.down ? "#40e5656e" : (root.hovered ? "#33e5656e" : theme.dangerBg)
            if (!root.primary)
                return root.down ? theme.borderSubtle : theme.cardBgRaised
            if (root.down)
                return Qt.darker(theme.accent, 1.15)
            if (root.hovered)
                return Qt.lighter(theme.accent, 1.08)
            return theme.accent
        }
        border.width: root.primary || root.danger ? 0 : 1
        border.color: root.hovered ? "#3a4250" : theme.borderSubtle
    }
}
