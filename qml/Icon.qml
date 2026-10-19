import QtQuick

// Line icons drawn from SVG path data on a 24×24 grid, so they render the
// same everywhere (no dependency on symbol fonts) and take any color.
Canvas {
    id: root
    property string name: ""
    property color color: "white"
    property real lineWidth: 1.8
    readonly property var paths: ({
        overview: "M3 12a9 9 0 1 0 18 0a9 9 0 1 0 -18 0M12 12L16 8M7 12h1M12 6.5v1",
        snapshots: "M3.5 12a8.5 8.5 0 1 0 2.5 -6M3 3.5v4.5h4.5M12 7.5v4.5l3 2",
        vault: "M5 11h14v10H5zM8 11V7.5a4 4 0 0 1 8 0V11M12 15v2",
        folders: "M3 6.5h6l2 2h10V19H3zM3 11h18",
        settings: "M4 7h9M17 7h3M15 5v4M4 12h3M11 12h9M9 10v4M4 17h11M19 17h1M17 15v4",
        space: "M12 3l9 5-9 5-9-5zM3 12.5l9 5 9-5M3 16.5l9 5 9-5"
    })

    implicitWidth: 18
    implicitHeight: 18
    onNameChanged: requestPaint()
    onColorChanged: requestPaint()
    onWidthChanged: requestPaint()
    onHeightChanged: requestPaint()

    onPaint: {
        const ctx = getContext("2d")
        ctx.reset()
        const data = root.paths[root.name]
        if (data === undefined)
            return
        ctx.scale(width / 24, height / 24)
        ctx.lineWidth = root.lineWidth
        ctx.lineCap = "round"
        ctx.lineJoin = "round"
        ctx.strokeStyle = root.color
        ctx.path = data
        ctx.stroke()
    }
}
