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
package org.apache.flink.runtime.rpc.resourcemanager;

import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;

import java.util.UUID;

/**
 * {@link ResourceManagerLeaderListener} is responsible to react when the leader of resource manager has been changed.
 * it will transfer the callback message to a rpc call on a {@link RpcGateway} implementing {@link CallBack}
 *
 * @param <T> a listener Endpoint acting as the listener of the leadership
 */
public class ResourceManagerLeaderListener<T extends RpcEndpoint<? extends ResourceManagerLeaderListener.CallBack>>
	implements LeaderRetrievalListener
{

	/** RpcEndpoint that implements the {@link CallBack} interface */
	private T endpoint;

	public ResourceManagerLeaderListener(T endpoint) {
		this.endpoint = endpoint;
	}

	/**
	 * Notification for new leader availability
	 *
	 * @param leaderAddress   The address of the new leader
	 * @param leaderSessionID The new leader session ID
	 */
	@Override
	public void notifyLeaderAddress(String leaderAddress, UUID leaderSessionID) {
		endpoint.getSelf().notifyOfNewResourceManagerLeader(leaderAddress, leaderSessionID);
	}

	/**
	 * Handle errors on ResourceManager leader retrieval service, which will trigger async error handling of endpoint
	 *
	 * @param exception
	 */
	@Override
	public void handleError(Exception exception) {
		endpoint.getSelf().notifyResourceManagerListenerError(exception);
	}

	/**
	 * a {@link RpcGateway} definition that concerns leader changes of ResourceManager.
	 */
	public interface CallBack extends RpcGateway {
		/**
		 * Callback interface used to notify a rpcGateway that the leader of resource manager has changed
		 *
		 * @param leaderAddress   the address of new leader, null if no leader at present
		 * @param leaderSessionID the leader session id of the new leader, null if no leader at present
		 */
		void notifyOfNewResourceManagerLeader(String leaderAddress, UUID leaderSessionID);

		/**
		 * Callback interface used to notify error of resource manager retrieval service
		 *
		 * @param cause the cause of the error
		 */
		void notifyResourceManagerListenerError(Throwable cause);
	}
}
