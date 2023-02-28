pub const MP_DATETIME: std::os::raw::c_char = 4;

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct datetime {
    pub epoch: f64,
    pub nsec: i32,
    pub tzoffset: i16,
    pub tzindex: i16,
}
