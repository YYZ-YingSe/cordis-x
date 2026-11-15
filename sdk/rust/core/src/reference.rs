//! Public object references have the same API in every Rust execution adapter.
//! The key is an opaque runtime capability carried by generated protobuf data;
//! it is never interpreted as a service name, process address or native pointer.
use crate::{Context, Descriptor, Message, ResourceObject, Result, backend::Operation};
use std::rc::Rc;

#[derive(Clone)]
pub struct InterfaceRef {
    key: String,
    descriptor: Descriptor,
    digest: String,
}
impl InterfaceRef {
    pub fn from_key(key: String, descriptor: Descriptor) -> Self {
        Self {
            key,
            digest: descriptor.digest(),
            descriptor,
        }
    }
    pub fn key(&self) -> &str {
        &self.key
    }

    /// A known refusal has not transferred ownership: the exporter still owns
    /// its implementation. Unknown admission retains the adapter slot for scope
    /// recovery. Exporting should precede work whose only inverse is close().
    pub async fn export(
        ctx: &Context,
        descriptor: Descriptor,
        object: Rc<dyn ResourceObject>,
    ) -> Result<Self> {
        let value = ctx
            .value(Operation::ReferenceExport {
                descriptor: descriptor.clone(),
                object,
            })
            .await?;
        let key = value
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| crate::typed::violation("missing interface reference"))?;
        Ok(Self::from_key(key, descriptor))
    }
    pub async fn invoke(&self, ctx: &Context, method: &str, args: Message) -> Result<Message> {
        ctx.message(Operation::ReferenceInvoke {
            key: self.key.clone(),
            digest: self.digest.clone(),
            descriptor: self.descriptor.clone(),
            method: method.into(),
            args,
        })
        .await
    }
    pub async fn dispose(&self, ctx: &Context) -> Result<()> {
        ctx.value(Operation::ReferenceDispose {
            key: self.key.clone(),
        })
        .await
        .map(|_| ())
    }
}
