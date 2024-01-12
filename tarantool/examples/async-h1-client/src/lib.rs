use http_types::{Method, Request, Url};
use tarantool::fiber;
use tarantool::network::client::tcp::TcpStream;
use tarantool::network::client::tcp::UnsafeSendSyncTcpStream;
use tarantool::proc;

#[proc]
fn get(url: &str) -> http_types::Result<()> {
    fiber::block_on(async {
        println!("Connecting...");
        let url = Url::parse(url)?;
        let host = url
            .host_str()
            .ok_or(http_types::Error::from_display("host not specified"))?;
        let req = Request::new(Method::Get, url.clone());
        let mut res = match url.scheme() {
            "http" => {
                let stream = TcpStream::connect(host, 80)
                    .await
                    .map_err(http_types::Error::from_display)?;
                let stream = UnsafeSendSyncTcpStream(stream);
                println!("Sending request over http...");
                async_h1::connect(stream, req).await?
            }
            #[cfg(feature = "tls")]
            "https" => {
                let stream = TcpStream::connect(host, 443)
                    .await
                    .map_err(http_types::Error::from_display)?;
                let stream = UnsafeSendSyncTcpStream(stream);
                let stream = async_native_tls::connect(host, stream).await?;
                println!("Sending request over https...");
                async_h1::connect(stream, req).await?
            }
            _ => {
                return Err(http_types::Error::from_display("scheme not supported"));
            }
        };

        println!("Response Status: {}", res.status());
        println!("Response Body: {}", res.body_string().await?);
        Ok(())
    })
}
