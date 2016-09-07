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

package org.apache.flink.runtime.rpc.akka;

import akka.actor.ActorSystem;
import akka.util.Timeout;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.rpc.exceptions.RpcConnectionException;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class AkkaRpcServiceTest extends TestLogger {

	// ------------------------------------------------------------------------
	//  shared test members
	// ------------------------------------------------------------------------

	private static ActorSystem actorSystem = AkkaUtils.createDefaultActorSystem();

	private static AkkaRpcService akkaRpcService =
		new AkkaRpcService(actorSystem, new Timeout(10000, TimeUnit.MILLISECONDS));

	@AfterClass
	public static void shutdown() {
		akkaRpcService.stopService();
		actorSystem.shutdown();
	}

	// ------------------------------------------------------------------------
	//  tests
	// ------------------------------------------------------------------------

	@Test
	public void testScheduleRunnable() throws Exception {
		final OneShotLatch latch = new OneShotLatch();
		final long delay = 100;
		final long start = System.nanoTime();

		akkaRpcService.scheduleRunnable(new Runnable() {
			@Override
			public void run() {
				latch.trigger();
			}
		}, delay, TimeUnit.MILLISECONDS);

		latch.await();
		final long stop = System.nanoTime();

		assertTrue("call was not properly delayed", ((stop - start) / 1000000) >= delay);
	}

	/**
	 * Test connect method
	 * 1. Get the same result when connect the same address and same gateway class
	 * @throws Exception
	 */
	@Test
	public void testConnect() throws Exception {
		TestingLeaderElectionService leaderElectionService = new TestingLeaderElectionService();
		TestingHighAvailabilityServices highAvailabilityServices = new TestingHighAvailabilityServices();
		highAvailabilityServices.setResourceManagerLeaderElectionService(leaderElectionService);

		ResourceManager rm = new ResourceManager(akkaRpcService, highAvailabilityServices);
		rm.start();
		String address = rm.getAddress();
		// verify get the same result when connect the same address and same gateway class
		Future<ResourceManagerGateway> rmGatewayFuture1 = akkaRpcService.connect(address, ResourceManagerGateway.class);
		ResourceManagerGateway rmGateway1 = Await.result(rmGatewayFuture1, new FiniteDuration(200, TimeUnit.MILLISECONDS));

		Future<ResourceManagerGateway> rmGatewayFuture2 = akkaRpcService.connect(address, ResourceManagerGateway.class);
		ResourceManagerGateway rmGateway2 = Await.result(rmGatewayFuture2, new FiniteDuration(200, TimeUnit.MILLISECONDS));

		Assert.assertEquals(rmGateway1, rmGateway2);
		Assert.assertEquals(rmGateway1.hashCode(), rmGateway2.hashCode());
	}

	/**
	 * Test connect method
	 * 1. Failed when connect to invalid address
	 * @throws Exception
	 */
	@Test(expected = RpcConnectionException.class)
	public void testConnectInvalidAddress() throws Exception {
		TestingLeaderElectionService leaderElectionService = new TestingLeaderElectionService();
		TestingHighAvailabilityServices highAvailabilityServices = new TestingHighAvailabilityServices();
		highAvailabilityServices.setResourceManagerLeaderElectionService(leaderElectionService);

		ResourceManager rm = new ResourceManager(akkaRpcService, highAvailabilityServices);
		rm.start();
		// verify failed when connect to invalid address
		String invalidString = rm.getAddress() + "abc";
		Future<ResourceManagerGateway> invalidRmGatewayFuture = akkaRpcService.connect(invalidString, ResourceManagerGateway.class);
		Await.result(invalidRmGatewayFuture, new FiniteDuration(200, TimeUnit.MILLISECONDS));
	}

}
