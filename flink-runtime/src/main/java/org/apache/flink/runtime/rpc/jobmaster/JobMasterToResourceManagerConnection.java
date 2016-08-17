/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rpc.jobmaster;

import akka.dispatch.OnFailure;
import akka.dispatch.OnSuccess;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.registration.RegistrationResponse;
import org.apache.flink.runtime.rpc.registration.RetryingRegistration;
import org.apache.flink.runtime.rpc.resourcemanager.ResourceManagerGateway;
import org.slf4j.Logger;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public class JobMasterToResourceManagerConnection {

	/** the logger for all log messages of this class */
	private final Logger log;

	/** the JobMaster whose connection to the ResourceManager this represents */
	private final JobMaster jobMaster;

	private final UUID resourceManagerLeaderId;

	private final String resourceManagerAddress;

	private ResourceManagerRegistration pendingRegistration;

	private ResourceManagerGateway registeredResourceManager;

	/** flag indicating that the connection is closed */
	private volatile boolean closed;

	/** messages return from resource manager when registered */
	private volatile long heartbeatInterval;

	public JobMasterToResourceManagerConnection(
			Logger log,
			JobMaster jobMaster,
			String resourceManagerAddress,
			UUID resourceManagerLeaderId) {

		this.log = checkNotNull(log);
		this.jobMaster = checkNotNull(jobMaster);
		this.resourceManagerAddress = checkNotNull(resourceManagerAddress);
		this.resourceManagerLeaderId = checkNotNull(resourceManagerLeaderId);
	}

	// ------------------------------------------------------------------------
	//  Life cycle
	// ------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	public void start() {
		checkState(!closed, "The connection is already closed");
		checkState(!isRegistered() && pendingRegistration == null, "The connection is already started");

		ResourceManagerRegistration registration = new ResourceManagerRegistration(
				log, jobMaster.getRpcService(),
				resourceManagerAddress, resourceManagerLeaderId,
				jobMaster.getAddress(), jobMaster.getJobId());
		registration.startRegistration();

		Future<Tuple2<ResourceManagerGateway, RegistrationSuccessResponse>> future = registration.getFuture();
		
		future.onSuccess(new OnSuccess<Tuple2<ResourceManagerGateway, RegistrationSuccessResponse>>() {
			@Override
			public void onSuccess(Tuple2<ResourceManagerGateway, RegistrationSuccessResponse> result) {
				registeredResourceManager = result.f0;
				heartbeatInterval = result.f1.heartbeatInterval;
			}
		}, jobMaster.getMainThreadExecutionContext());
		
		// this future should only ever fail if there is a bug, not if the registration is declined
		future.onFailure(new OnFailure() {
			@Override
			public void onFailure(Throwable failure) {
				jobMaster.onFatalError(failure);
			}
		}, jobMaster.getMainThreadExecutionContext());
	}

	public void close() {
		closed = true;

		// make sure we do not keep re-trying forever
		if (pendingRegistration != null) {
			pendingRegistration.cancel();
		}
	}

	public boolean isClosed() {
		return closed;
	}

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	public UUID getResourceManagerLeaderId() {
		return resourceManagerLeaderId;
	}

	public String getResourceManagerAddress() {
		return resourceManagerAddress;
	}

	/**
	 * Gets the ResourceManagerGateway. This returns null until the registration is completed.
	 */
	public ResourceManagerGateway getResourceManager() {
		return registeredResourceManager;
	}

	public boolean isRegistered() {
		return registeredResourceManager != null;
	}

	public long getHeartbeatInterval() {
		return heartbeatInterval;
	}
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return String.format("Connection to ResourceManager %s (leaderId=%s)",
				resourceManagerAddress, resourceManagerLeaderId); 
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	/**
	 * {@link RetryingRegistration} implementation that trigger JobMaster register RPC call
	 */
	static class ResourceManagerRegistration
			extends RetryingRegistration<ResourceManagerGateway, RegistrationSuccessResponse> {

		private final String jobMasterAddress;
		
		private final JobID jobID;

		public ResourceManagerRegistration(
				Logger log,
				RpcService rpcService,
				String targetAddress,
				UUID leaderId,
				String jobMasterAddress,
				JobID jobID) {

			super(log, rpcService, "ResourceManager", ResourceManagerGateway.class, targetAddress, leaderId);
			this.jobMasterAddress = checkNotNull(jobMasterAddress);
			this.jobID = checkNotNull(jobID);
		}

		@Override
		protected Future<RegistrationResponse> invokeRegistration(
				ResourceManagerGateway resourceManager, UUID leaderId, long timeoutMillis) throws Exception {

			FiniteDuration timeout = new FiniteDuration(timeoutMillis, TimeUnit.MILLISECONDS);

			return resourceManager.registerJobMaster(leaderId, jobMasterAddress, jobID, timeout);
		}
	}

	/**
	 * ResourceManager response message when a JobMaster succeeds in registering.
	 */
	public static class RegistrationSuccessResponse extends RegistrationResponse.Success {
		private final long heartbeatInterval;

		public RegistrationSuccessResponse(long heartbeatInterval) {
			this.heartbeatInterval = heartbeatInterval;
		}
	}
}
