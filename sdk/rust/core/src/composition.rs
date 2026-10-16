use crate::{
    Context, Message, Result,
    backend::{Handle, Operation},
};

#[derive(Clone)]
pub struct Child(Handle);
impl Child {
    pub async fn ready(&self, ctx: &Context) -> Result<()> {
        ctx.value(Operation::ChildReady {
            child: self.0.clone(),
        })
        .await
        .map(|_| ())
    }
    pub async fn dispose(&self, ctx: &Context) -> Result<()> {
        ctx.value(Operation::ChildDispose {
            child: self.0.clone(),
        })
        .await
        .map(|_| ())
    }
}
impl Context {
    pub async fn install(&self, factory: &str, config: Message) -> Result<Child> {
        self.submit(Operation::Install {
            factory: factory.into(),
            config,
        })
        .await?
        .handle()
        .map(Child)
    }
    pub async fn provide(&self, service: &str, value: Message) -> Result<()> {
        self.value(Operation::Provide {
            service: service.into(),
            value,
        })
        .await
        .map(|_| ())
    }
}
