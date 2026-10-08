import QtQuick
import app.verfsnext 1.0

Item {
    id: root
    readonly property VerfsTheme theme: VerfsTheme {}
    property color color: theme.accent
    property bool running: visible
    implicitWidth: 18
    implicitHeight: 18

    Canvas {
        id: canvas
        anchors.fill: parent
        onPaint: {
            const ctx = getContext("2d")
            ctx.reset()
            const r = Math.min(width, height) / 2 - 2
            ctx.lineWidth = 2.5
            ctx.lineCap = "round"
            ctx.strokeStyle = root.color
            ctx.beginPath()
            ctx.arc(width / 2, height / 2, r, 0, Math.PI * 1.4)
            ctx.stroke()
        }
        RotationAnimator on rotation {
            from: 0
            to: 360
            duration: 900
            loops: Animation.Infinite
            running: root.running
        }
    }
}
