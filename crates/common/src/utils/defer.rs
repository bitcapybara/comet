pub struct Defer<F: Fn()>(F);

pub fn defer<F: Fn()>(f: F) -> Defer<F> {
    Defer(f)
}

impl<F> Drop for Defer<F>
where
    F: Fn(),
{
    fn drop(&mut self) {
        (self.0)()
    }
}
