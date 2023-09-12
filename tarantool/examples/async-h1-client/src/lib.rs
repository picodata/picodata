use http_types::{Method, Request, Url};
use tarantool::fiber;
use tarantool::network::client::tcp::TcpStream;
use tarantool::proc;

#[proc]
fn get(url: &str) -> http_types::Result<()> {
    fiber::block_on(async {
        println!("Connecting...");
        let stream = TcpStream::connect(url.strip_prefix("http://").unwrap(), 80)
            .await
            .map_err(http_types::Error::from_display)?;
        let url = Url::parse(url)?;

        println!("Sending request...");
        let req = Request::new(Method::Get, url);
        let mut res = async_h1::connect(stream, req).await?;
        println!("Response Status: {}", res.status());
        println!("Response Body: {}", res.body_string().await?);
        Ok(())
    })
}
