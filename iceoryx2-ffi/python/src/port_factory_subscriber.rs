// Copyright (c) 2025 Contributors to the Eclipse Foundation
//
// See the NOTICE file(s) distributed with this work for additional
// information regarding copyright ownership.
//
// This program and the accompanying materials are made available under the
// terms of the Apache Software License 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0, or the MIT license
// which is available at https://opensource.org/licenses/MIT.
//
// SPDX-License-Identifier: Apache-2.0 OR MIT

use iceoryx2::service::builder::{CustomHeaderMarker, CustomPayloadMarker};
use pyo3::prelude::*;

use crate::{
    error::SubscriberCreateError,
    parc::Parc,
    port_factory_pipeline::PortFactoryPipelineType,
    port_factory_publish_subscribe::PortFactoryPublishSubscribeType,
    subscriber::{Subscriber, SubscriberType},
    type_storage::TypeStorage,
};

type IpcPortFactorySubscriber<'a> =
    iceoryx2::service::port_factory::subscriber::PortFactorySubscriber<
        'a,
        crate::IpcService,
        [CustomPayloadMarker],
        CustomHeaderMarker,
    >;
type LocalPortFactorySubscriber<'a> =
    iceoryx2::service::port_factory::subscriber::PortFactorySubscriber<
        'a,
        crate::LocalService,
        [CustomPayloadMarker],
        CustomHeaderMarker,
    >;

pub(crate) enum PortFactorySubscriberType {
    Ipc(Parc<IpcPortFactorySubscriber<'static>>),
    Local(Parc<LocalPortFactorySubscriber<'static>>),
}

pub(crate) enum PortFactorySubscriberFactoryType {
    PublishSubscribe(Parc<PortFactoryPublishSubscribeType>),
    Pipeline(Parc<PortFactoryPipelineType>),
}

#[pyclass]
/// Factory to create a new `Subscriber` port/endpoint for
/// `MessagingPattern::PublishSubscribe` based communication.
pub struct PortFactorySubscriber {
    factory: PortFactorySubscriberFactoryType,
    value: PortFactorySubscriberType,
    payload_type_details: TypeStorage,
    user_header_type_details: TypeStorage,
}

impl PortFactorySubscriber {
    pub(crate) fn new(
        factory: Parc<PortFactoryPublishSubscribeType>,
        payload_type_details: TypeStorage,
        user_header_type_details: TypeStorage,
    ) -> Self {
        Self {
            factory: PortFactorySubscriberFactoryType::PublishSubscribe(factory.clone()),
            value: match &*factory.lock() {
                PortFactoryPublishSubscribeType::Ipc(v) => PortFactorySubscriberType::Ipc(unsafe {
                    Parc::new(core::mem::transmute::<
                        IpcPortFactorySubscriber<'_>,
                        IpcPortFactorySubscriber<'static>,
                    >(v.subscriber_builder()))
                }),
                PortFactoryPublishSubscribeType::Local(v) => {
                    PortFactorySubscriberType::Local(unsafe {
                        Parc::new(core::mem::transmute::<
                            LocalPortFactorySubscriber<'_>,
                            LocalPortFactorySubscriber<'static>,
                        >(v.subscriber_builder()))
                    })
                }
            },
            payload_type_details,
            user_header_type_details,
        }
    }

    pub(crate) fn from_pipeline_worker(
        factory: Parc<PortFactoryPipelineType>,
        stage_id: usize,
        payload_type_details: TypeStorage,
        user_header_type_details: TypeStorage,
    ) -> Option<Self> {
        let value = match &*factory.lock() {
            PortFactoryPipelineType::Ipc(v) => {
                let builder = v.__internal_worker_subscriber_builder(stage_id)?;
                PortFactorySubscriberType::Ipc(unsafe {
                    Parc::new(core::mem::transmute::<
                        IpcPortFactorySubscriber<'_>,
                        IpcPortFactorySubscriber<'static>,
                    >(builder))
                })
            }
            PortFactoryPipelineType::Local(v) => {
                let builder = v.__internal_worker_subscriber_builder(stage_id)?;
                PortFactorySubscriberType::Local(unsafe {
                    Parc::new(core::mem::transmute::<
                        LocalPortFactorySubscriber<'_>,
                        LocalPortFactorySubscriber<'static>,
                    >(builder))
                })
            }
        };

        Some(Self {
            factory: PortFactorySubscriberFactoryType::Pipeline(factory),
            value,
            payload_type_details,
            user_header_type_details,
        })
    }

    pub(crate) fn from_pipeline_egress(
        factory: Parc<PortFactoryPipelineType>,
        payload_type_details: TypeStorage,
        user_header_type_details: TypeStorage,
    ) -> Self {
        Self {
            factory: PortFactorySubscriberFactoryType::Pipeline(factory.clone()),
            value: match &*factory.lock() {
                PortFactoryPipelineType::Ipc(v) => PortFactorySubscriberType::Ipc(unsafe {
                    Parc::new(core::mem::transmute::<
                        IpcPortFactorySubscriber<'_>,
                        IpcPortFactorySubscriber<'static>,
                    >(
                        v.__internal_egress_subscriber_builder()
                    ))
                }),
                PortFactoryPipelineType::Local(v) => PortFactorySubscriberType::Local(unsafe {
                    Parc::new(core::mem::transmute::<
                        LocalPortFactorySubscriber<'_>,
                        LocalPortFactorySubscriber<'static>,
                    >(
                        v.__internal_egress_subscriber_builder()
                    ))
                }),
            },
            payload_type_details,
            user_header_type_details,
        }
    }

    fn with_factory_lock<R>(&self, callback: impl FnOnce() -> R) -> R {
        match &self.factory {
            PortFactorySubscriberFactoryType::PublishSubscribe(factory) => {
                let _guard = factory.lock();
                callback()
            }
            PortFactorySubscriberFactoryType::Pipeline(factory) => {
                let _guard = factory.lock();
                callback()
            }
        }
    }
}

impl PortFactorySubscriber {
    fn clone_ipc(&self, value: IpcPortFactorySubscriber<'static>) -> Self {
        Self {
            factory: match &self.factory {
                PortFactorySubscriberFactoryType::PublishSubscribe(factory) => {
                    PortFactorySubscriberFactoryType::PublishSubscribe(factory.clone())
                }
                PortFactorySubscriberFactoryType::Pipeline(factory) => {
                    PortFactorySubscriberFactoryType::Pipeline(factory.clone())
                }
            },
            value: PortFactorySubscriberType::Ipc(Parc::new(value)),
            payload_type_details: self.payload_type_details.clone(),
            user_header_type_details: self.user_header_type_details.clone(),
        }
    }

    fn clone_local(&self, value: LocalPortFactorySubscriber<'static>) -> Self {
        Self {
            factory: match &self.factory {
                PortFactorySubscriberFactoryType::PublishSubscribe(factory) => {
                    PortFactorySubscriberFactoryType::PublishSubscribe(factory.clone())
                }
                PortFactorySubscriberFactoryType::Pipeline(factory) => {
                    PortFactorySubscriberFactoryType::Pipeline(factory.clone())
                }
            },
            value: PortFactorySubscriberType::Local(Parc::new(value)),
            payload_type_details: self.payload_type_details.clone(),
            user_header_type_details: self.user_header_type_details.clone(),
        }
    }
}

#[pymethods]
impl PortFactorySubscriber {
    /// Defines the buffer size of the `Subscriber`. Smallest possible value is `1`.
    pub fn buffer_size(&self, value: usize) -> Self {
        self.with_factory_lock(|| match &self.value {
            PortFactorySubscriberType::Ipc(v) => {
                let this = unsafe { (*v.lock()).__internal_partial_clone() };
                let this = this.buffer_size(value);
                self.clone_ipc(this)
            }
            PortFactorySubscriberType::Local(v) => {
                let this = unsafe { (*v.lock()).__internal_partial_clone() };
                let this = this.buffer_size(value);
                self.clone_local(this)
            }
        })
    }

    /// Creates a new `Subscriber` or emits a `SubscriberCreateError` on failure.
    pub fn create(&self) -> PyResult<Subscriber> {
        self.with_factory_lock(|| match &self.value {
            PortFactorySubscriberType::Ipc(v) => {
                let this = unsafe { (*v.lock()).__internal_partial_clone() };
                Ok(Subscriber {
                    value: Parc::new(SubscriberType::Ipc(Some(
                        this.create()
                            .map_err(|e| SubscriberCreateError::new_err(format!("{e:?}")))?,
                    ))),
                    payload_type_details: self.payload_type_details.clone(),
                    user_header_type_details: self.user_header_type_details.clone(),
                })
            }
            PortFactorySubscriberType::Local(v) => {
                let this = unsafe { (*v.lock()).__internal_partial_clone() };
                Ok(Subscriber {
                    value: Parc::new(SubscriberType::Local(Some(
                        this.create()
                            .map_err(|e| SubscriberCreateError::new_err(format!("{e:?}")))?,
                    ))),
                    payload_type_details: self.payload_type_details.clone(),
                    user_header_type_details: self.user_header_type_details.clone(),
                })
            }
        })
    }
}
