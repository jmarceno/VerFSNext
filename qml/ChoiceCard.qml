import QtQuick
import QtQuick.Controls
import QtQuick.Layouts
import app.verfsnext 1.0

Rectangle {
    id: root
    readonly property VerfsTheme theme: VerfsTheme {}
    property string title: ""
    property string description: ""
    property string badge: ""
    property bool checked: false
    signal clicked()

    radius: theme.radius
    color: root.checked ? theme.accentSoft : (mouse.containsMouse ? theme.cardBgRaised : theme.cardBg)
    border.width: root.checked ? 2 : 1
    border.color: root.checked ? theme.accent : theme.borderSubtle
    implicitHeight: row.implicitHeight + 36

    RowLayout {
        id: row
        anchors.left: parent.left
        anchors.right: parent.right
        anchors.verticalCenter: parent.verticalCenter
        anchors.margins: 18
        spacing: 16

        Rectangle {
            Layout.alignment: Qt.AlignTop
            Layout.topMargin: 2
            width: 20
            height: 20
            radius: 10
            color: "transparent"
            border.width: 2
            border.color: root.checked ? theme.accent : theme.textDim
            Rectangle {
                anchors.centerIn: parent
                width: 10
                height: 10
                radius: 5
                color: theme.accent
                visible: root.checked
            }
        }

        ColumnLayout {
            Layout.fillWidth: true
            spacing: 6
            RowLayout {
                spacing: 10
                Label {
                    text: root.title
                    color: theme.textPrimary
                    font.pixelSize: 15
                    font.bold: true
                }
                Rectangle {
                    visible: root.badge.length > 0
                    radius: 9
                    color: theme.accentMuted
                    implicitHeight: 20
                    implicitWidth: badgeLabel.implicitWidth + 16
                    Label {
                        id: badgeLabel
                        anchors.centerIn: parent
                        text: root.badge
                        color: theme.accentStrong
                        font.pixelSize: 11
                        font.bold: true
                    }
                }
            }
            Label {
                Layout.fillWidth: true
                text: root.description
                color: theme.textSecondary
                font.pixelSize: 13
                wrapMode: Text.WordWrap
                lineHeight: 1.15
            }
        }
    }

    MouseArea {
        id: mouse
        anchors.fill: parent
        hoverEnabled: true
        cursorShape: Qt.PointingHandCursor
        onClicked: root.clicked()
    }
}
