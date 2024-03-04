pub struct Defer<F>(F)
where
    F: Fn();

pub fn defer<F>(f: F) -> Defer<F>
where
    F: Fn(),
{
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
