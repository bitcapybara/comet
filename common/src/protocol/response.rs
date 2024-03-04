#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Response(pub ReturnCode);

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[repr(u8)]
pub enum ReturnCode {
    Success = 0,
    EndpointClosed,
}
