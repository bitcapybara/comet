use crate::protocol::response::Response;

pub trait ResponsiveError: common_error::StackError {
    fn as_response(&self) -> Response;
}

impl<T> From<T> for Response
where
    T: ResponsiveError,
{
    fn from(value: T) -> Self {
        value.as_response()
    }
}

/// An opaque boxed error based on errors that implement [ErrorExt] trait.
pub struct BoxedError {
    inner: Box<dyn ResponsiveError + Send + Sync + 'static>,
}

impl BoxedError {
    pub fn new<E: ResponsiveError + Send + Sync + 'static>(err: E) -> Self {
        Self {
            inner: Box::new(err),
        }
    }
}

impl std::fmt::Debug for BoxedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl std::fmt::Display for BoxedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl std::error::Error for BoxedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.inner.source()
    }
}

impl common_error::StackError for BoxedError {
    fn debug_fmt(&self, layer: usize, buf: &mut Vec<String>) {
        self.inner.debug_fmt(layer, buf)
    }

    fn next(&self) -> Option<&dyn common_error::StackError> {
        self.inner.next()
    }
}

impl ResponsiveError for BoxedError {
    fn as_response(&self) -> Response {
        self.inner.as_response()
    }
}
