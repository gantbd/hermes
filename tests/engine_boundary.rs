use std::process::Command;

#[test]
fn engine_stays_independent_of_host_effect_and_mechanism_dependencies() {
    let output = Command::new(env!("CARGO"))
        .args([
            "tree",
            "-p",
            "hermes-ledger",
            "--edges",
            "normal",
            "--prefix",
            "none",
            "--locked",
            "--offline",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let tree = String::from_utf8(output.stdout).unwrap();
    assert_eq!(
        tree.lines().count(),
        1,
        "effect dependency entered the engine: {tree}"
    );
}
