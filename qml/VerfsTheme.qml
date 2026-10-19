import QtQuick

QtObject {
    // Shared palette with GravaAI / Lepramim / Celestial (same color values).
    // Qt 8-digit hex is #AARRGGBB (alpha first), not CSS #RRGGBBAA.
    readonly property color windowBg: "#121418"
    readonly property color cardBg: "#1b1e25"
    readonly property color cardBgRaised: "#232833"
    readonly property color accent: "#2fb3a3"
    readonly property color accentMuted: "#332fb3a3"
    readonly property color accentSoft: "#222fb3a3"
    readonly property color accentStrong: "#3fd4c1"
    readonly property color accentInk: "#0d1f1c"
    readonly property color textPrimary: "#ffffff"
    readonly property color textSecondary: "#c5c9ce"
    readonly property color textMuted: "#8b929a"
    readonly property color textDim: "#6b7280"
    readonly property color statusGreen: "#3ecf8e"
    readonly property color statusGreenBg: "#223ecf8e"
    readonly property color danger: "#e5656e"
    readonly property color dangerBg: "#26e5656e"
    readonly property color dangerStrong: "#f07880"
    readonly property color warning: "#e8b339"
    readonly property color warningBg: "#26e8b339"
    readonly property color borderSubtle: "#2a303b"
    readonly property color inputBg: "#12151b"
    readonly property color sliderTrack: "#2a2f38"
    readonly property int radius: 10
    readonly property int radiusSm: 8
    readonly property int sidebarWidth: 236
    readonly property string iconSource: "qrc:/qt/qml/app/verfsnext/assets/verfsnext.svg"

    function bytes(n) {
        if (n === undefined || n === null || isNaN(n))
            return "—"
        const units = ["B", "KB", "MB", "GB", "TB", "PB"]
        let v = Math.abs(n)
        let i = 0
        while (v >= 1024 && i < units.length - 1) {
            v /= 1024
            i++
        }
        const digits = (i === 0 || v >= 100) ? 0 : (v >= 10 ? 1 : 2)
        return (n < 0 ? "-" : "") + v.toFixed(digits) + " " + units[i]
    }

    function percent(fraction) {
        if (fraction === undefined || fraction === null || isNaN(fraction))
            return "—"
        return (fraction * 100).toFixed(fraction >= 0.1 ? 0 : 1) + "%"
    }

    function duration(secs) {
        if (secs === undefined || secs === null || isNaN(secs))
            return "—"
        const s = Math.floor(secs)
        const d = Math.floor(s / 86400)
        const h = Math.floor((s % 86400) / 3600)
        const m = Math.floor((s % 3600) / 60)
        if (d > 0)
            return d + "d " + h + "h"
        if (h > 0)
            return h + "h " + m + "m"
        if (m > 0)
            return m + "m"
        return s + "s"
    }

    function stateLabel(state) {
        switch (state) {
        case "running": return "Running"
        case "starting": return "Starting…"
        case "stopping": return "Stopping…"
        case "failed": return "Needs attention"
        default: return "Stopped"
        }
    }

    function stateColor(state) {
        switch (state) {
        case "running": return statusGreen
        case "starting":
        case "stopping": return warning
        case "failed": return danger
        default: return textDim
        }
    }
}
