use super::*;
use cordis_loader::Phase;
use cordis_remote::loader::register_process;

impl Files {
    async fn wait(&self, version: &str, event: &str) {
        bounded(async {
            while self.count(version, event) == 0 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await;
    }
    pub(super) fn cpp_artifact(&self, root: &std::path::Path, version: &str) {
        let build = binary().parent().unwrap().to_owned();
        std::fs::copy(
            build.join(format!("cordis_package_process_{version}")),
            root.join("program"),
        )
        .unwrap();
        let helper = format!(
            "{}cordis_process_helper_{version}_{version}{}",
            std::env::consts::DLL_PREFIX,
            std::env::consts::DLL_SUFFIX
        );
        std::fs::copy(build.join(&helper), root.join(&helper)).unwrap();
        let leaf = format!(
            "{}cordis_process_leaf_{version}_{version}{}",
            std::env::consts::DLL_PREFIX,
            std::env::consts::DLL_SUFFIX
        );
        std::fs::copy(build.join(&leaf), root.join(&leaf)).unwrap();
        PackageManifest::from_files(
            root,
            "program".into(),
            vec![
                ("program".into(), FileKind::Executable),
                (helper, FileKind::Library),
                (leaf, FileKind::Library),
            ],
            &package_policy::example_policy(self.0.join("staged")),
        )
        .unwrap()
        .write(root)
        .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_hash_and_failed_handshake_or_activation_never_change_the_live_route() {
    let app = Rolling::new(Backend::Cpp, Endpoint::Ipc).await;
    let before = app.loader.snapshot();
    let mut bad = app.new.clone();
    bad.artifact.as_mut().unwrap().sha256 = "0".repeat(64);
    assert!(
        app.loader
            .apply(Configuration {
                entries: vec![app.old.clone(), bad]
            })
            .await
            .is_err()
    );
    assert_eq!(
        app.loader.snapshot().desired_revision,
        before.desired_revision
    );
    assert_eq!(app.files.staged(), 1);
    // Valid bytes with a different exported contract pass file preflight, then fail hello.
    let adder = binary().with_file_name("cordis_adder");
    let mut bad = app.new.clone();
    let root = app.files.0.join("wrong-contract");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::copy(adder, root.join("program")).unwrap();
    bad.artifact = Some(
        PackageManifest::from_files(
            &root,
            "program".into(),
            vec![("program".into(), FileKind::Executable)],
            &app.files.options(Backend::Cpp).package,
        )
        .unwrap()
        .write(&root)
        .unwrap(),
    );
    bad.config["plain_args"] = json!(true);
    let applied = app
        .loader
        .apply(Configuration {
            entries: vec![app.old.clone(), bad],
        })
        .await;
    let failure = match applied {
        Ok(_) => app.loader.settled().await.unwrap_err().to_string(),
        Err(error) => error.to_string(),
    };
    assert!(failure.contains("contract_mismatch"), "{failure}");
    assert_eq!(
        app.call("code-version", serde_json::json!({}))
            .await
            .unwrap(),
        "v1"
    );
    let mut bad = app.new.clone();
    bad.config["fail_activate"] = json!(true);
    app.loader
        .apply(Configuration {
            entries: vec![app.old.clone(), bad],
        })
        .await
        .unwrap();
    assert!(app.loader.settled().await.is_err());
    assert_eq!(app.tokens.lock().unwrap().len(), 1);
    assert_eq!(
        app.call("code-version", serde_json::json!({}))
            .await
            .unwrap(),
        "v1"
    );
    app.switch().await;
    app.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_call_and_acquisition_waits_keep_old_session_until_late_work_recovers() {
    let app = Rolling::new(Backend::Cpp, Endpoint::Http).await;
    let control = app.counter(false).await;
    let late = Arc::new(Mutex::new(None));
    let wait = tokio::spawn({
        let handle = app.handle.clone();
        let late = late.clone();
        async move {
            handle
                .route(move |s, ctx| {
                    Box::pin(async move {
                        *late.lock().unwrap() = Some(
                            s.acquire_json(&ctx, "counter", json!({"wait":"late"}))
                                .await?,
                        );
                        Ok(())
                    })
                })
                .await
        }
    });
    app.files.wait("v1", "hold:late").await;
    wait.abort();
    let _ = wait.await;
    app.switch().await;
    let retire = app.retire();
    app.closing().await;
    assert_eq!(app.files.count("v1", "deactivated"), 0);
    control.invoke_json("release", json!("late")).await.unwrap();
    bounded(async {
        while late.lock().unwrap().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let resource = late.lock().unwrap().take().unwrap();
    assert_eq!(
        resource
            .invoke_json("code-version", serde_json::json!({}))
            .await
            .unwrap(),
        "v1"
    );
    resource.dispose().await.unwrap();
    control.dispose().await.unwrap();
    bounded(retire).await.unwrap().unwrap();
    assert_eq!(app.files.count("v1", "deactivated"), 1);
    app.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_old_inverse_is_cached_and_blocks_exit_while_v2_serves() {
    let app = Rolling::new(Backend::Cpp, Endpoint::Ipc).await;
    let resource = app.counter(true).await;
    app.switch().await;
    let retire = app.retire();
    app.closing().await;
    let first = resource.dispose().await.unwrap_err();
    assert_eq!(resource.dispose().await.unwrap_err(), first);
    assert!(bounded(retire).await.unwrap().is_err());
    assert_eq!(app.files.count("v1", "resource_closed:counter"), 1);
    assert_eq!(app.files.count("v1", "deactivated"), 0);
    let state = app.loader.snapshot();
    assert_eq!(state.phase, Phase::Failed);
    assert!(
        state
            .entries
            .iter()
            .find(|e| e.id == "v1")
            .unwrap()
            .actual
            .is_some()
    );
    assert_eq!(
        app.call("code-version", serde_json::json!({}))
            .await
            .unwrap(),
        "v2"
    );
    assert!(app.rt.shutdown().await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn old_process_crash_preserves_unknown_resource_without_faulting_new_route() {
    let app = Rolling::new(Backend::Cpp, Endpoint::Http).await;
    let resource = app.counter(false).await;
    app.switch().await;
    assert!(
        resource
            .invoke_json("crash", serde_json::json!({}))
            .await
            .is_err()
    );
    assert!(resource.dispose().await.is_err());
    assert!(bounded(app.retire()).await.unwrap().is_err());
    assert_eq!(
        app.call("code-version", serde_json::json!({}))
            .await
            .unwrap(),
        "v2"
    );
    assert_eq!(app.consumer.snapshot().state, Lifecycle::Active);
    assert!(app.rt.shutdown().await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_handoff_rechecks_preview_and_pins_accepted_code_through_cancelled_apply() {
    let files = Files::new();
    let old = files.artifact(Backend::Cpp, "v1");
    let new = files.artifact(Backend::Cpp, "v2");
    let rt = Runtime::new();
    let saved = Arc::new(Mutex::new(None));
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let mut registry = Registry::new();
    register_process(
        &mut registry,
        "process",
        calculator(),
        files.options(Backend::Cpp),
        |_| {
            let mut spec = ProcessSpec::new("");
            spec.args.push("--stdio-only".into());
            Ok(spec)
        },
    )
    .unwrap();
    registry
        .register("client", {
            let saved = saved.clone();
            let entered = entered.clone();
            let release = release.clone();
            move |_| {
                let saved = saved.clone();
                let entered = entered.clone();
                let release = release.clone();
                Ok(Plugin::new("client", move |ctx| {
                    let saved = saved.clone();
                    let entered = entered.clone();
                    let release = release.clone();
                    async move {
                        let handle = ctx.dependency(&calculator())?;
                        let observed = version(&handle).await;
                        *saved.lock().unwrap() = Some(handle);
                        ctx.defer("consumer uses old process", move |cleanup| async move {
                            assert_eq!(
                                version(&cleanup.dependency(&calculator())?).await,
                                observed
                            );
                            if observed == "v1" {
                                entered.add_permits(1);
                                release.acquire().await.unwrap().forget();
                            }
                            Ok(())
                        })?;
                        Ok(())
                    }
                })
                .requires(&calculator()))
            }
        })
        .unwrap();
    let unchanged = rt
        .install(Plugin::new("unrelated", |_| async { Ok(()) }))
        .unwrap();
    let stable = unchanged.ready().await.unwrap().activation;
    let loader = Loader::new(rt.clone(), registry);
    let config = |artifact| Configuration {
        entries: vec![
            Entry::new("process", "process", Value::Null).artifact(artifact),
            Entry::new("client", "client", Value::Null),
        ],
    };
    loader.apply(config(old.clone())).await.unwrap();
    loader.settled().await.unwrap();
    let first: ServiceHandle<RemoteService> = saved.lock().unwrap().as_ref().unwrap().clone();
    let resource = acquire(&first, Endpoint::Ipc, json!({})).await.unwrap();
    loader.plan(config(new.clone())).await.unwrap();
    let new_bytes = std::fs::read(&new.location).unwrap();
    std::fs::copy(&old.location, &new.location).unwrap();
    let before = loader.snapshot().desired_revision;
    assert!(loader.apply(config(new.clone())).await.is_err());
    assert_eq!(loader.snapshot().desired_revision, before);
    std::fs::write(&new.location, new_bytes).unwrap();
    let wait = tokio::spawn({
        let loader = loader.clone();
        let config = config(new.clone());
        async move { loader.apply(config).await }
    });
    bounded(entered.acquire()).await.unwrap().forget();
    let snapshot = loader.snapshot();
    let entry = snapshot.entries.iter().find(|e| e.id == "process").unwrap();
    assert_eq!(
        entry.actual.as_ref().unwrap().artifact.as_ref().unwrap(),
        &old
    );
    assert_eq!(
        entry.desired.as_ref().unwrap().artifact.as_ref().unwrap(),
        &new
    );
    std::fs::copy(&old.location, &new.location).unwrap();
    wait.abort();
    let _ = wait.await;
    release.add_permits(1);
    bounded(loader.settled()).await.unwrap();
    let current = saved.lock().unwrap().as_ref().unwrap().clone();
    assert_eq!(version(&current).await, "v2");
    assert!(
        resource
            .invoke_json("get", serde_json::json!({}))
            .await
            .is_err()
    );
    assert!(
        call(&first, Endpoint::Ipc, "echo", Value::Null)
            .await
            .is_err()
    );
    assert_eq!(unchanged.snapshot().activation, stable);
    bounded(rt.shutdown()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_ordinary_process_cleanup_retains_actual_and_never_starts_replacement() {
    let files = Files::new();
    let root = files.0.clone();
    let rt = Runtime::new();
    let old = files.artifact(Backend::Cpp, "v1");
    let new = files.artifact(Backend::Cpp, "v2");
    let mut registry = Registry::new();
    register_process(
        &mut registry,
        "process",
        calculator(),
        files.options(Backend::Cpp),
        move |config| {
            let version = config.as_str().unwrap();
            let mut spec = ProcessSpec::new("");
            spec.args = vec![
                "--stdio-only".into(),
                "--trace".into(),
                root.join(format!("{version}.log")).display().to_string(),
            ];
            if version == "v1" {
                spec.args.push("--fail-deactivate".into());
            }
            Ok(spec)
        },
    )
    .unwrap();
    let loader = Loader::new(rt.clone(), registry);
    loader
        .apply(Configuration {
            entries: vec![Entry::new("process", "process", json!("v1")).artifact(old.clone())],
        })
        .await
        .unwrap();
    loader.settled().await.unwrap();
    let target = Configuration {
        entries: vec![Entry::new("process", "process", json!("v2")).artifact(new.clone())],
    };
    assert!(bounded(loader.apply(target.clone())).await.is_err());
    assert!(bounded(loader.apply(target)).await.is_err());
    let snapshot = loader.snapshot();
    let entry = &snapshot.entries[0];
    assert_eq!(
        entry.actual.as_ref().unwrap().artifact.as_ref().unwrap(),
        &old
    );
    assert_eq!(
        entry.desired.as_ref().unwrap().artifact.as_ref().unwrap(),
        &new
    );
    assert_eq!(files.count("v1", "deactivated"), 1);
    assert_eq!(files.count("v2", "activated"), 0);
    assert!(rt.shutdown().await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broker_reverse_calls_keep_version_dependencies_and_caller_effects_after_cancelled_wait() {
    struct Log(&'static str);
    let logger = Service::<Log>::new("example.version-log", "1");
    let service = Service::<Broker<RemoteService>>::new("example.calculator", "1").with_descriptor(
        cordis_contract::portable::decode(
            &cordis_example_contracts::resources::calculator_logging::SERVICE
                .parse()
                .unwrap(),
        )
        .unwrap(),
    );
    let files = Files::new();
    let rt = Runtime::new();
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let effects = Arc::new(Mutex::new(Vec::new()));
    let recovered = Arc::new(AtomicUsize::new(0));
    let broker = rt
        .install(
            Plugin::new("broker", {
                let service = service.clone();
                move |ctx| {
                    let service = service.clone();
                    async move { ctx.provide_broker(&service) }
                }
            })
            .provides(&service),
        )
        .unwrap();
    broker.ready().await.unwrap();
    let (consumer, saved) = client(&rt, &service).await;
    let stable = consumer.snapshot().activation;
    let handle = saved.lock().unwrap().take().unwrap();
    let mut providers = Vec::new();
    let mut tokens = Vec::new();
    for label in ["v1", "v2"] {
        let view = View::new().isolate(&logger);
        let log = rt
            .install_in(
                view.clone(),
                Plugin::new("logger", {
                    let logger = logger.clone();
                    move |ctx| {
                        let logger = logger.clone();
                        async move { ctx.provide(&logger, Arc::new(Log(label))) }
                    }
                })
                .provides(&logger),
            )
            .unwrap();
        log.ready().await.unwrap();
        let mut spec = ProcessSpec::new("").requires(cordis_remote::Dependency::new_json(
            &logger,
            version_log_descriptor(),
            {
                let entered = entered.clone();
                let release = release.clone();
                let effects = effects.clone();
                let recovered = recovered.clone();
                move |log, ctx, method, args| {
                    let entered = entered.clone();
                    let release = release.clone();
                    let effects = effects.clone();
                    let recovered = recovered.clone();
                    Box::pin(async move {
                        assert_eq!(method, "write");
                        assert_eq!(args, log.0);
                        if log.0 == "v1" {
                            entered.add_permits(1);
                            release.acquire().await.unwrap().forget();
                            let effect = ctx
                                .defer("old reverse contribution", move |_| async move {
                                    recovered.fetch_add(1, Ordering::SeqCst);
                                    Ok(())
                                })
                                .map_err(|e| cordis_remote::Error::Resource(e.to_string()))?;
                            effects.lock().unwrap().push(effect);
                        }
                        Ok(json!(log.0))
                    })
                }
            },
        ));
        spec.args = vec!["--stdio-only".into(), "--logger".into()];
        let code = ProcessArtifact::prepare(
            &files.artifact(Backend::Cpp, label),
            &files.options(Backend::Cpp),
        )
        .unwrap();
        let token = Arc::new(Mutex::new(None));
        let provider = rt
            .install_in(
                view,
                code.broker_plugin(label, &service, spec, {
                    let token = token.clone();
                    move |t| {
                        *token.lock().unwrap() = Some(t);
                        Ok(())
                    }
                })
                .unwrap(),
            )
            .unwrap();
        provider.ready().await.unwrap();
        providers.push(provider);
        tokens.push(token.lock().unwrap().take().unwrap());
    }
    tokens[0].promote().unwrap();
    let observed = Arc::new(Mutex::new(None));
    let wait = tokio::spawn({
        let handle = handle.clone();
        let observed = observed.clone();
        async move {
            handle
                .route(move |s, ctx| {
                    Box::pin(async move {
                        *observed.lock().unwrap() = Some(
                            s.invoke_json(&ctx, "log-version", serde_json::json!({}))
                                .await?,
                        );
                        Ok(())
                    })
                })
                .await
        }
    });
    bounded(entered.acquire()).await.unwrap().forget();
    wait.abort();
    let _ = wait.await;
    tokens[1].promote().unwrap();
    assert_eq!(
        handle
            .route(|s, ctx| Box::pin(async move {
                s.invoke_json(&ctx, "log-version", serde_json::json!({}))
                    .await
                    .map_err(Into::into)
            }))
            .await
            .unwrap(),
        "v2"
    );
    let retiring = tokio::spawn({
        let p = providers[0].clone();
        async move { p.dispose().await }
    });
    bounded(async {
        while providers[0].snapshot().state != Lifecycle::Unloading {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(!retiring.is_finished());
    release.add_permits(1);
    bounded(async {
        while observed.lock().unwrap().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(*observed.lock().unwrap(), Some(json!("v1")));
    assert!(!retiring.is_finished());
    let effect = effects.lock().unwrap().pop().unwrap();
    effect.dispose().await.unwrap();
    bounded(retiring).await.unwrap().unwrap();
    assert_eq!(recovered.load(Ordering::SeqCst), 1);
    assert_eq!(consumer.snapshot().activation, stable);
    bounded(rt.shutdown()).await.unwrap();
}

fn version_log_descriptor() -> cordis_remote::Descriptor {
    cordis_remote::Descriptor::new(cordis_remote::Interface {
        controls: Default::default(),
        methods: cordis_remote::Dispatch::Named(
            [(
                "write".into(),
                cordis_remote::Method {
                    args: cordis_remote::MessageContract::standard("google.protobuf.StringValue"),
                    result: cordis_remote::MessageContract::standard("google.protobuf.StringValue"),
                    errors: Default::default(),
                },
            )]
            .into(),
        ),
        resources: cordis_remote::Dispatch::Named(Default::default()),
        capabilities: Default::default(),
    })
    .unwrap()
}
