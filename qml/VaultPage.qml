import QtQuick
import QtQuick.Controls
import QtQuick.Layouts
import app.verfsnext 1.0

ScrollView {
    id: root
    readonly property VerfsTheme theme: VerfsTheme {}
    required property var controller
    required property FolderPicker picker
    readonly property bool running: controller.daemon_state === "running"
    readonly property var status: controller.status_json.length > 0 ? JSON.parse(controller.status_json) : null
    readonly property string mode: {
        if (!running || !status)
            return "offline"
        if (!status.vault_enabled)
            return "disabled"
        if (!status.vault_initialized)
            return "create"
        return status.vault_locked ? "locked" : "open"
    }
    property string keyDir: controller.home_dir
    property string pickTarget: ""
    signal openSettings()

    contentWidth: availableWidth
    clip: true

    function strength(pw) {
        let score = 0
        if (pw.length >= 8) score++
        if (pw.length >= 12) score++
        if (/[A-Z]/.test(pw) && /[a-z]/.test(pw)) score++
        if (/[0-9]/.test(pw)) score++
        if (/[^A-Za-z0-9]/.test(pw)) score++
        return score
    }

    Connections {
        target: root.picker
        function onPicked(path) {
            if (root.pickTarget === "keyDir")
                root.keyDir = path
            else if (root.pickTarget === "keyFile")
                unlockKey.text = path
            root.pickTarget = ""
        }
    }

    ColumnLayout {
        width: root.availableWidth
        spacing: 16

        Card {
            Layout.fillWidth: true
            title: {
                switch (root.mode) {
                case "offline": return "Your vault is available while VerFSNext runs"
                case "disabled": return "The vault is turned off"
                case "create": return "Keep private files in an encrypted vault"
                case "locked": return "Your vault is locked"
                default: return "Your vault is open"
                }
            }
            subtitle: {
                switch (root.mode) {
                case "offline": return "Start VerFSNext to create, unlock or lock your vault."
                case "disabled": return "Turn on “Allow an encrypted vault” in Settings to use it."
                case "create": return "The vault is a hidden .vault folder inside VerFSNext. Its files are encrypted with your password and a key file, and it stays invisible while locked."
                case "locked": return "Enter your password and choose your key file to show the .vault folder."
                default: return "The .vault folder is visible and usable. Lock it when you're done; it also locks when VerFSNext stops."
                }
            }

            TealButton {
                visible: root.mode === "disabled"
                text: "Open Settings"
                primary: false
                onClicked: root.openSettings()
            }

            RowLayout {
                visible: root.mode === "open"
                spacing: 10
                TealButton {
                    text: "Open Vault"
                    onClicked: root.controller.openVault()
                }
                TealButton {
                    text: "Lock Vault"
                    primary: false
                    enabled: !root.controller.busy
                    onClicked: root.controller.lockVault()
                }
            }
        }

        // Create
        Card {
            visible: root.mode === "create"
            Layout.fillWidth: true
            title: "Create your vault"

            ColumnLayout {
                width: parent.width
                spacing: 14
                Field {
                    id: createPw
                    Layout.fillWidth: true
                    label: "Password"
                    echoMode: TextInput.Password
                    hint: {
                        const s = root.strength(text)
                        if (text.length === 0) return "Use at least 12 characters. A few unrelated words work well."
                        if (s <= 2) return "Weak password"
                        if (s <= 3) return "Okay password"
                        return "Strong password"
                    }
                }
                Field {
                    id: createPw2
                    Layout.fillWidth: true
                    label: "Confirm password"
                    echoMode: TextInput.Password
                    error: text.length > 0 && text !== createPw.text ? "Passwords don't match." : ""
                }
                PathField {
                    Layout.fillWidth: true
                    label: "Save the key file in"
                    text: root.keyDir
                    hint: "A small file named verfsnext.vault.key. You need it, together with your password, to unlock the vault."
                    onEdited: root.keyDir = text
                    onBrowseRequested: {
                        root.pickTarget = "keyDir"
                        root.picker.pickFiles = false
                        root.picker.prompt = "Pick where to save your vault key file. A USB drive or another safe place is a good choice."
                        root.picker.openAt(root.keyDir)
                    }
                }
                Rectangle {
                    Layout.fillWidth: true
                    radius: theme.radiusSm
                    color: theme.warningBg
                    implicitHeight: warnText.implicitHeight + 24
                    Label {
                        id: warnText
                        anchors.left: parent.left
                        anchors.right: parent.right
                        anchors.verticalCenter: parent.verticalCenter
                        anchors.margins: 12
                        text: "Keep a backup of the key file and remember your password. If either is lost, nobody — including you — can open the vault again."
                        color: theme.warning
                        font.pixelSize: 13
                        wrapMode: Text.WordWrap
                    }
                }
                TealButton {
                    text: "Create Vault"
                    enabled: !root.controller.busy && createPw.text.length >= 8 && createPw.text === createPw2.text && root.keyDir.length > 0
                    onClicked: {
                        root.controller.createVault(createPw.text, root.keyDir)
                        createPw.text = ""
                        createPw2.text = ""
                    }
                }
                Label {
                    visible: createPw.text.length > 0 && createPw.text.length < 8
                    text: "The password needs at least 8 characters."
                    color: theme.textMuted
                    font.pixelSize: 12
                }
            }
        }

        // Unlock
        Card {
            visible: root.mode === "locked"
            Layout.fillWidth: true
            title: "Unlock"

            ColumnLayout {
                width: parent.width
                spacing: 14
                Field {
                    id: unlockPw
                    Layout.fillWidth: true
                    label: "Password"
                    echoMode: TextInput.Password
                    onAccepted: unlockBtn.clicked()
                }
                PathField {
                    id: unlockKey
                    Layout.fillWidth: true
                    label: "Key file"
                    text: root.controller.vault_key_file
                    hint: "The verfsnext.vault.key file created with the vault."
                    onBrowseRequested: {
                        root.pickTarget = "keyFile"
                        root.picker.pickFiles = true
                        root.picker.prompt = "Find your verfsnext.vault.key file."
                        root.picker.openAt(unlockKey.text)
                    }
                }
                TealButton {
                    id: unlockBtn
                    text: "Unlock Vault"
                    enabled: !root.controller.busy && unlockPw.text.length > 0 && unlockKey.text.length > 0
                    onClicked: {
                        root.controller.unlockVault(unlockPw.text, unlockKey.text)
                        unlockPw.text = ""
                    }
                }
            }
        }
    }
}
