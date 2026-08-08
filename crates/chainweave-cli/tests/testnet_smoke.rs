use std::process::Command;

#[test]
#[ignore = "requires CHAINWEAVE_TESTNET_RPC_URL and public network access"]
fn head_reads_real_testnet_rpc() {
    let rpc_url = std::env::var("CHAINWEAVE_TESTNET_RPC_URL")
        .expect("set CHAINWEAVE_TESTNET_RPC_URL to opt into this test");
    let output = Command::new(env!("CARGO_BIN_EXE_chainweave"))
        .args(["--rpc-url", &rpc_url, "head"])
        .output()
        .expect("chainweave binary should run");

    assert!(
        output.status.success(),
        "chainweave head failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("head output should be UTF-8");
    assert!(stdout.contains("\"chain_id\""));
    assert!(stdout.contains("\"genesis_hash\""));
}
