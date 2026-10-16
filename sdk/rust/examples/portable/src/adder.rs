//! Business logic over standard prost messages; conversion is generated.
use super::generated::{
    AdderAddError, AdderExport, AdderServer,
    data::{
        cordis::data::{Int64, Unit},
        example::AddArgs,
    },
};
use cordis_sdk_core::{Context, LocalFuture, Result};
use std::rc::Rc;
pub struct Adder;
pub type Plugin = AdderExport<Adder>;
impl AdderServer for Adder {
    fn create(_: Unit) -> Result<Self> {
        Ok(Self)
    }
    fn add(self: Rc<Self>, _: Context, args: AddArgs) -> LocalFuture<Int64> {
        Box::pin(async move {
            let value = args
                .left
                .unwrap()
                .checked_add(args.right.unwrap())
                .ok_or_else(|| AdderAddError::Overflow.failure("int64 addition overflow"))?;
            Ok(Int64 { value: Some(value) })
        })
    }
}
