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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.RpcMethod;
import org.apache.flink.runtime.rpc.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.resourcemanager.ResourceManagerLeaderListener;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;

import java.util.UUID;
import java.util.concurrent.ExecutorService;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * JobMaster implementation. The job master is responsible for the execution of a single
 * {@link org.apache.flink.runtime.jobgraph.JobGraph}.
 *
 * It offers the following methods as part of its rpc interface to interact with the JobMaster
 * remotely:
 * <ul>
 *     <li>{@link #updateTaskExecutionState(TaskExecutionState)} updates the task execution state for
 * given task</li>
 * </ul>
 */
public class JobMaster extends RpcEndpoint<JobMasterGateway> {
	/** The access to the leader election and metadata storage services */
	private final HighAvailabilityServices haServices;

	/** The rpc server address of Resource Manager */
	private String resourceManagerAddress;
	/** The leader uuid of Resource Manager */
	private UUID resourceManagerLeaderUUID;
	/** The resource manager connection */
	private JobMasterToResourceManagerConnection rmConnection;

	/** The running JobGraph */
	private final JobGraph jobGraph;

	public JobMaster(
		JobGraph jobGraph,
		HighAvailabilityServices haServices,
		RpcService rpcService,
		ExecutorService executorService) {
		super(rpcService);
		this.jobGraph = checkNotNull(jobGraph);
		this.haServices = checkNotNull(haServices);
	}

	public ResourceManagerGateway getResourceManager() {
		return rmConnection.getResourceManager();
	}

	@Override
	public void start() {
		super.start();
		startLeaderElection();
		registerAtResourceManager();
	}

	private void startLeaderElection() {

	}

	private void registerAtResourceManager() {
		try {
			haServices.getResourceManagerLeaderRetriever().start(new ResourceManagerLeaderListener(this));
		} catch (Exception e) {
			onFatalError(e);
		}

	}

	//----------------------------------------------------------------------------------------------
	//  RPC methods - ResourceManager related
	//----------------------------------------------------------------------------------------------

	/**
	 * Callback triggered by leader retrieval listener when resource manager leader changed
	 * @param leaderAddress
	 * @param leaderSessionID
	 */
	@RpcMethod
	public void notifyOfNewResourceManagerLeader(String leaderAddress, UUID leaderSessionID) {
		this.resourceManagerAddress = leaderAddress;
		this.resourceManagerLeaderUUID = leaderSessionID;
		resetResourceManagerConnection();
	}

	/**
	 * Handler resource manager listener error message
	 * @param cause The Exception causes the error
	 */
	@RpcMethod
	public void notifyResourceManagerListenerError(Throwable cause) {
		//TODO: Maybe need to poison the job master or restart a leader retrieval service.
		onFatalError(cause);
	}

	//----------------------------------------------------------------------------------------------
	// RPC methods - TaskManager related
	//----------------------------------------------------------------------------------------------

	/**
	 * Updates the task execution state for a given task.
	 *
	 * @param taskExecutionState New task execution state for a given task
	 * @return Acknowledge the task execution state update
	 */

	@RpcMethod
	public Acknowledge updateTaskExecutionState(TaskExecutionState taskExecutionState) {
		System.out.println("TaskExecutionState: " + taskExecutionState);
		return Acknowledge.get();
	}

	/**
	 * Trigger the registration of the job master at the resource manager.
	 */
	public void resetResourceManagerConnection() {
		// close connection to prior resource manager
		if(rmConnection != null) {
			rmConnection.close();
			rmConnection = null;
		}

		// if current address is not a leader, do not register
		if(!isLeader()) {
			return;
		}

		// connect to new resource manager
		if(resourceManagerAddress != null && resourceManagerLeaderUUID != null) {
			rmConnection = new JobMasterToResourceManagerConnection(
				log,
				this,
				resourceManagerAddress,
				resourceManagerLeaderUUID);

			rmConnection.start();
		}
	}

	//----------------------------------------------------------------------------------------------
	// Helper methods
	//----------------------------------------------------------------------------------------------


	// ------------------------------------------------------------------------
	//  Error handling
	// ------------------------------------------------------------------------

	/**
	 * Notifies the TaskExecutor that a fatal error has occurred and it cannot proceed.
	 * This method should be used when asynchronous threads want to notify the
	 * Endpoint of a fatal error.
	 *
	 * @param t The exception describing the fatal error
	 */
	protected void onFatalErrorAsync(final Throwable t) {
		runAsync(new Runnable() {
			@Override
			public void run() {
				onFatalError(t);
			}
		});
	}

	/**
	 * Notifies the TaskExecutor that a fatal error has occurred and it cannot proceed.
	 * This method must only be called from within the TaskExecutor's main thread.
	 *
	 * @param t The exception describing the fatal error
	 */
	protected void onFatalError(Throwable t) {
		// to be determined, probably delegate to a fatal error handler that
		// would either log (mini cluster) ot kill the process (yarn, mesos, ...)
		log.error("FATAL ERROR", t);
	}

	/**
	 * Return JobID of the job running in JobMaster
	 * @return
	 */
	public JobID getJobId() {
		return this.jobGraph.getJobID();
	}

	/**
	 * Return if the job master is connected to a resource manager.
	 *
	 * @return true if the job master is connected to the resource manager
	 */
	public boolean isConnected() {
		return rmConnection != null && rmConnection.isRegistered();
	}

	/**
	 * Check whether current job master is the leader master
	 * @return
	 */
	private boolean isLeader() {
		//TODO check whether current job master is a leader master from HAservices
		return true;
	};
}
