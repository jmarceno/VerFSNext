import QtQuick
import QtQuick.Controls
import QtQuick.Layouts
import QtQuick.Window
import app.verfsnext 1.0

Rectangle {
    id: root
    readonly property VerfsTheme theme: VerfsTheme {}
    required property Window window
    property string title: "VerFSNext"
    property bool canMaximize: true
    signal closeRequested()

    implicitHeight: 44
    color: theme.windowBg

    MouseArea {
        anchors.fill: parent
        acceptedButtons: Qt.LeftButton
        onPressed: root.window.startSystemMove()
        onDoubleClicked: {
            if (!root.canMaximize)
                return
            if (root.window.visibility === Window.Maximized)
                root.window.showNormal()
            else
                root.window.showMaximized()
        }
    }

    RowLayout {
        anchors.fill: parent
        anchors.leftMargin: 14
        anchors.rightMargin: 10
        spacing: 10

        Image {
            source: theme.iconSource
            sourceSize.width: 22
            sourceSize.height: 22
            Layout.preferredWidth: 22
            Layout.preferredHeight: 22
            fillMode: Image.PreserveAspectFit
        }

        Label {
            text: root.title
            color: theme.textPrimary
            font.pixelSize: 15
            font.bold: true
            Layout.fillWidth: true
        }

        Row {
            spacing: 4
            Repeater {
                model: root.canMaximize ? ["min", "max", "close"] : ["min", "close"]
                delegate: Rectangle {
                    required property string modelData
                    width: 32
                    height: 28
                    radius: 6
                    color: winBtn.containsMouse
                           ? (modelData === "close" ? "#e35d6a" : theme.cardBgRaised)
                           : "transparent"
                    Label {
                        anchors.centerIn: parent
                        text: modelData === "min" ? "—" : (modelData === "max" ? "□" : "✕")
                        color: theme.textSecondary
                        font.pixelSize: 12
                    }
                    MouseArea {
                        id: winBtn
                        anchors.fill: parent
                        hoverEnabled: true
                        cursorShape: Qt.PointingHandCursor
                        onClicked: {
                            if (modelData === "min")
                                root.window.showMinimized()
                            else if (modelData === "max") {
                                if (root.window.visibility === Window.Maximized)
                                    root.window.showNormal()
                                else
                                    root.window.showMaximized()
                            } else
                                root.closeRequested()
                        }
                    }
                }
            }
        }
    }
}
