use http_types::{Method, Request, Url};
use tarantool::error::Error;
use tarantool::fiber;
use tarantool::network::client::tcp::TcpStream;
use tarantool::network::client::tcp::UnsafeSendSyncTcpStream;
use tarantool::proc;

#[proc]
fn get(url: &str) -> Result<(), Error> {
    fiber::block_on(async {
        println!("Connecting...");
        let url = Url::parse(url).map_err(Error::other)?;
        let host = url
            .host_str()
            .ok_or_else(|| Error::other("host not specified"))?;
        let req = Request::new(Method::Get, url.clone());
        let mut res = match url.scheme() {
            "http" => {
                let stream = TcpStream::connect(host, 80).map_err(Error::other)?;
                let stream = UnsafeSendSyncTcpStream(stream);
                println!("Sending request over http...");
                async_h1::connect(stream, req).await.map_err(Error::other)?
            }
            #[cfg(feature = "tls")]
            "https" => {
                let stream = TcpStream::connect(host, 443).map_err(Error::other)?;
                let stream = UnsafeSendSyncTcpStream(stream);
                let stream = async_native_tls::connect(host, stream)
                    .await
                    .map_err(Error::other)?;
                println!("Sending request over https...");
                async_h1::connect(stream, req).await.map_err(Error::other)?
            }
            _ => {
                return Err(Error::other("scheme not supported"));
            }
        };

        println!("Response Status: {}", res.status());
        println!(
            "Response Body: {}",
            res.body_string().await.map_err(Error::other)?
        );
        Ok(())
    })
}
