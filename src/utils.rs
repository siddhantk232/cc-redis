macro_rules! write_response {
    ($stream:expr, $response:expr) => {
        let mut encoder = crate::resp::RespParser;
        let mut response = Default::default();

        let _ = encoder.encode($response, &mut response).unwrap();
        $stream.write(&response).await.unwrap();
    };
}
