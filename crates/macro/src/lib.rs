mod stack_error;

use proc_macro::TokenStream;

#[proc_macro_attribute]
pub fn stack_error_debug(args: TokenStream, input: TokenStream) -> TokenStream {
    stack_error::stack_trace_style_impl(args.into(), input.into()).into()
}
