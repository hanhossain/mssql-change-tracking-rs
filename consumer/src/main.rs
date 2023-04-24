use common::Tester;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    let tester = Tester::create_from_env().await;

    loop {
        let tracking_version = tester.get_tracking_version().await;
        println!("Current tracking version: {tracking_version}");

        let last_version = tester.get_last_tracked_version().await;
        println!("Last tracked version: {last_version:?}");

        let changes = tester.get_changes(last_version).await;
        for change in changes {
            println!("{:?}", change);
        }

        tester
            .set_last_tracked_version(tracking_version, last_version.is_some())
            .await;

        sleep(Duration::from_secs(3)).await;
    }
}
