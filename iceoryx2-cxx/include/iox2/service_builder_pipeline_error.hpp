// Copyright (c) 2026 Contributors to the Eclipse Foundation
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

#ifndef IOX2_SERVICE_BUILDER_PIPELINE_ERROR_HPP
#define IOX2_SERVICE_BUILDER_PIPELINE_ERROR_HPP

#include <cstdint>

namespace iox2 {
/// Errors that can occur when an existing [`MessagingPattern::Pipeline`] [`Service`] shall be opened.
enum class PipelineOpenError : uint8_t {
    /// The [`Service`] could not be opened since it does not exist.
    DoesNotExist,
    /// The process has insufficient permissions to open the [`Service`].
    InsufficientPermissions,
    /// Some underlying resources of the [`Service`] are either missing, corrupted or inaccessible.
    ServiceInCorruptedState,
    /// The [`Service`] has the wrong messaging pattern.
    IncompatibleMessagingPattern,
    /// The [`AttributeVerifier`] required attributes that the [`Service`] does not satisfy.
    IncompatibleAttributes,
    /// The [`Service`] has an incompatible payload type.
    IncompatiblePayloadType,
    /// The [`Service`] creation timeout has passed and it is still not initialized.
    HangsInCreation,
    /// The [`Service`] supports less [`Node`](crate::node::Node)s than requested.
    DoesNotSupportRequestedAmountOfNodes,
    /// The [`Service`] supports fewer stages than requested.
    DoesNotSupportRequestedAmountOfStages,
    /// The [`Service`] supports fewer in-flight samples than requested.
    DoesNotSupportRequestedInFlightSamples,
    /// The [`Service`] supports a smaller initial max slice length than requested.
    DoesNotSupportRequestedInitialMaxSliceLen,
    /// The maximum number of [`Node`](crate::node::Node)s have already opened the [`Service`].
    ExceedsMaxNumberOfNodes,
    /// The [`Service`] is marked for destruction and currently cleaning up.
    IsMarkedForDestruction,
    /// The pipeline configuration is invalid.
    InvalidConfiguration,
    /// One internal edge service failed to open.
    EdgeFailure,
    /// Errors that indicate either an implementation issue or a wrongly configured system.
    InternalFailure,
};

/// Errors that can occur when a new [`MessagingPattern::Pipeline`] [`Service`] shall be created.
enum class PipelineCreateError : uint8_t {
    /// Some underlying resources of the [`Service`] are either missing, corrupted or inaccessible.
    ServiceInCorruptedState,
    /// Errors that indicate either an implementation issue or a wrongly configured system.
    InternalFailure,
    /// Multiple processes are trying to create the same [`Service`].
    IsBeingCreatedByAnotherInstance,
    /// The [`Service`] already exists.
    AlreadyExists,
    /// The [`Service`] creation timeout has passed and it is still not initialized.
    HangsInCreation,
    /// The process has insufficient permissions to create the [`Service`].
    InsufficientPermissions,
    /// The pipeline configuration is invalid.
    InvalidConfiguration,
    /// One internal edge service failed to create.
    EdgeFailure,
};

/// Errors that can occur when a [`MessagingPattern::Pipeline`] [`Service`] shall be created or opened.
enum class PipelineOpenOrCreateError : uint8_t {
    /// The [`Service`] could not be opened since it does not exist.
    OpenDoesNotExist,
    /// The process has insufficient permissions to open the [`Service`].
    OpenInsufficientPermissions,
    /// Some underlying resources of the [`Service`] are either missing, corrupted or inaccessible.
    OpenServiceInCorruptedState,
    /// The [`Service`] has the wrong messaging pattern.
    OpenIncompatibleMessagingPattern,
    /// The [`AttributeVerifier`] required attributes that the [`Service`] does not satisfy.
    OpenIncompatibleAttributes,
    /// The [`Service`] has an incompatible payload type.
    OpenIncompatiblePayloadType,
    /// The [`Service`] creation timeout has passed and it is still not initialized.
    OpenHangsInCreation,
    /// The [`Service`] supports less [`Node`](crate::node::Node)s than requested.
    OpenDoesNotSupportRequestedAmountOfNodes,
    /// The [`Service`] supports fewer stages than requested.
    OpenDoesNotSupportRequestedAmountOfStages,
    /// The [`Service`] supports fewer in-flight samples than requested.
    OpenDoesNotSupportRequestedInFlightSamples,
    /// The [`Service`] supports a smaller initial max slice length than requested.
    OpenDoesNotSupportRequestedInitialMaxSliceLen,
    /// The maximum number of [`Node`](crate::node::Node)s have already opened the [`Service`].
    OpenExceedsMaxNumberOfNodes,
    /// The [`Service`] is marked for destruction and currently cleaning up.
    OpenIsMarkedForDestruction,
    /// The pipeline configuration is invalid.
    OpenInvalidConfiguration,
    /// One internal edge service failed to open.
    OpenEdgeFailure,
    /// Errors that indicate either an implementation issue or a wrongly configured system.
    OpenInternalFailure,

    /// Some underlying resources of the [`Service`] are either missing, corrupted or inaccessible.
    CreateServiceInCorruptedState,
    /// Errors that indicate either an implementation issue or a wrongly configured system.
    CreateInternalFailure,
    /// Multiple processes are trying to create the same [`Service`].
    CreateIsBeingCreatedByAnotherInstance,
    /// The [`Service`] already exists.
    CreateAlreadyExists,
    /// The [`Service`] creation timeout has passed and it is still not initialized.
    CreateHangsInCreation,
    /// The process has insufficient permissions to create the [`Service`].
    CreateInsufficientPermissions,
    /// The pipeline configuration is invalid.
    CreateInvalidConfiguration,
    /// One internal edge service failed to create.
    CreateEdgeFailure,

    /// Can occur when another process creates and removes the same [`Service`] repeatedly with a high frequency.
    SystemInFlux,
};

} // namespace iox2

#endif
