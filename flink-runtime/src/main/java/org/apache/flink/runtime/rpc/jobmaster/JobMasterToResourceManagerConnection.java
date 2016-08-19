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
import scala.concurrent.Promise;
import scala.concurrent.duration.FiniteDuration;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link JobMasterToResourceManagerConnection} is responsible to maintain rpc gateway of resource manager for
 * JobMaster, including
 */
public class JobMasterToResourceManagerConnection {

	/** the logger for all log messages of this class */
	private final Logger log;

	/** the JobMaster whose connection to the ResourceManager this represents */
	private final JobMaster jobMaster;

	private final UUID resourceManagerLeaderId;

	private final String resourceManagerAddress;

	private ResourceManagerRegistration pendingRegistration;

	private Promise<ResourceManagerGateway> resourceManagerGatewayPromise;
	private ResourceManagerGateway registeredResourceManager;

	/** flag indicating that the connection is closed */
	private volatile boolean closed;

	public JobMasterToResourceManagerConnection(
		Logger log,
		JobMaster jobMaster,
		String resourceManagerAddress,
		UUID resourceManagerLeaderId)
	{

		this.log = checkNotNull(log);
		this.jobMaster = checkNotNull(jobMaster);
		this.resourceManagerAddress = checkNotNull(resourceManagerAddress);
		this.resourceManagerLeaderId = checkNotNull(resourceManagerLeaderId);
		this.resourceManagerGatewayPromise = new scala.concurrent.impl.Promise.DefaultPromise<>();
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

		Future<Tuple2<ResourceManagerGateway, RegistrationResponse.Success>> future = registration.getFuture();

		future.onSuccess(new OnSuccess<Tuple2<ResourceManagerGateway, RegistrationResponse.Success>>() {
			@Override
			public void onSuccess(Tuple2<ResourceManagerGateway, RegistrationResponse.Success> result) {
				registeredResourceManager = result.f0;
				resourceManagerGatewayPromise.success(registeredResourceManager);
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

	/**
	 * Close the connection to resource manager
	 */
	public void close() {
		closed = true;

		// make sure we do not keep re-trying forever
		if (pendingRegistration != null) {
			pendingRegistration.cancel();
		}

		// reset the registered gateway
		registeredResourceManager = null;

		// fail the promise of gateway
		if(resourceManagerGatewayPromise.isCompleted()) {
			resourceManagerGatewayPromise = new scala.concurrent.impl.Promise.DefaultPromise<>();
		}
		resourceManagerGatewayPromise.failure(new IllegalStateException("Connection closed"));
	}

	/**
	 * Check whether the connection is closed or not
	 * @return
	 */
	public boolean isClosed() {
		return closed;
	}

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	/**
	 * Gets the leader session id of the resource manager
	 * @return
	 */
	public UUID getResourceManagerLeaderId() {
		return resourceManagerLeaderId;
	}

	/**
	 * Gets the address of ResourceManagerGateway trying to connect.
	 *
	 * @return
	 */
	public String getResourceManagerAddress() {
		return resourceManagerAddress;
	}

	/**
	 * Gets the ResourceManagerGateway. This returns null until the registration is completed.
	 *
	 * @return
	 */
	public ResourceManagerGateway getResourceManager() {
		return registeredResourceManager;
	}

	/**
	 * Gets the future of ResourceManagerGateway. This return a future of resource manager gateway
	 * which will be complete when job master succeeds in registering to resource manager
	 *
	 * @return Future of that registered ResourceManagerGateway
	 */
	public Future<ResourceManagerGateway> getResourceManagerFuture() {
		return resourceManagerGatewayPromise.future();
	}

	/**
	 * Checks whether the job master is registered to resource master or not
	 * @return True for registered and fale for not
	 */
	public boolean isRegistered() {
		return registeredResourceManager != null;
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
		extends RetryingRegistration<ResourceManagerGateway, RegistrationResponse.Success>
	{

		private final String jobMasterAddress;

		private final JobID jobID;

		public ResourceManagerRegistration(
			Logger log,
			RpcService rpcService,
			String targetAddress,
			UUID leaderId,
			String jobMasterAddress,
			JobID jobID)
		{

			super(log, rpcService, "ResourceManager", ResourceManagerGateway.class, targetAddress, leaderId);
			this.jobMasterAddress = checkNotNull(jobMasterAddress);
			this.jobID = checkNotNull(jobID);
		}

		@Override
		protected Future<RegistrationResponse> invokeRegistration(
			ResourceManagerGateway resourceManager, UUID leaderId, long timeoutMillis) throws Exception
		{

			FiniteDuration timeout = new FiniteDuration(timeoutMillis, TimeUnit.MILLISECONDS);
			return resourceManager.registerJobMaster(leaderId, jobMasterAddress, jobID, timeout);
		}
	}

}
