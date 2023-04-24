use common::Tester;

#[tokio::main]
async fn main() {
    println!("Hello, world!");
    let tester = Tester::create_from_env().await;
    let tracking_version = tester.get_tracking_version().await;
    println!("Tracking version: {tracking_version}");
}
