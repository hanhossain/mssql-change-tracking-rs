use common::Tester;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    let tester = Tester::create_from_env().await;

    let mut counter = 0;
    if let Some(value) = tester.get_value("a").await {
        counter = value;
    } else {
        println!("Inserting id 'a' with value '{counter}'");
        tester.insert_value("a", counter).await;
        counter += 1;
        sleep(Duration::from_secs(1)).await;
    }

    loop {
        println!("Updating id 'a' with value '{counter}'");
        tester.update_value("a", counter).await;
        counter += 1;
        sleep(Duration::from_secs(1)).await;
    }
}
