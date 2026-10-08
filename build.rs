fn main() {
    #[cfg(feature = "gui")]
    build_gui();
}

#[cfg(feature = "gui")]
fn build_gui() {
    use cxx_qt_build::{CxxQtBuilder, QmlFile, QmlModule};

    // Every QML file must be listed here AND live directly in qml/ (no
    // subdirectories): cxx-qt 0.10 + Qt 6.2 misresolves module types from
    // subdirectories ("X is not a type" at runtime, lint-clean).
    CxxQtBuilder::new_qml_module(QmlModule::new("app.verfsnext").qml_files([
        QmlFile::from("qml/Main.qml"),
        QmlFile::from("qml/VerfsTheme.qml"),
        QmlFile::from("qml/SetupWindow.qml"),
        QmlFile::from("qml/ControlWindow.qml"),
        QmlFile::from("qml/Sidebar.qml"),
        QmlFile::from("qml/NavItem.qml"),
        QmlFile::from("qml/Card.qml"),
        QmlFile::from("qml/TealButton.qml"),
        QmlFile::from("qml/TealSwitch.qml"),
        QmlFile::from("qml/StatusDot.qml"),
        QmlFile::from("qml/Field.qml"),
        QmlFile::from("qml/PathField.qml"),
        QmlFile::from("qml/StatTile.qml"),
        QmlFile::from("qml/ChoiceCard.qml"),
        QmlFile::from("qml/StepDots.qml"),
        QmlFile::from("qml/FolderPicker.qml"),
        QmlFile::from("qml/ConfirmPopup.qml"),
        QmlFile::from("qml/TitleBar.qml"),
        QmlFile::from("qml/Toast.qml"),
        QmlFile::from("qml/Spinner.qml"),
        QmlFile::from("qml/Icon.qml"),
        QmlFile::from("qml/SettingsEditor.qml"),
        QmlFile::from("qml/SettingRow.qml"),
        QmlFile::from("qml/OverviewPage.qml"),
        QmlFile::from("qml/SnapshotsPage.qml"),
        QmlFile::from("qml/VaultPage.qml"),
        QmlFile::from("qml/StoragePage.qml"),
        QmlFile::from("qml/SettingsPage.qml"),
    ]))
    // Every Rust file containing a #[cxx_qt::bridge].
    .files(["src/gui/controller.rs"])
    .qt_module("Quick")
    .qt_module("QuickControls2")
    .qt_module("Svg")
    // Loadable from QML as "qrc:/qt/qml/app/verfsnext/assets/verfsnext.svg".
    .qrc_resources(["assets/verfsnext.svg"])
    .build();
}
