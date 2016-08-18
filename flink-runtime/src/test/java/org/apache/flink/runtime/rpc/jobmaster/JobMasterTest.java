package org.apache.flink.runtime.rpc.jobmaster;

import akka.actor.ActorSystem;
import akka.util.Timeout;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.NonHaServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.rpc.resourcemanager.ResourceManager;
import org.junit.AfterClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class JobMasterTest {
	// ------------------------------------------------------------------------
	//  shared test members
	// ------------------------------------------------------------------------
	// TODO user mocked rpc service instead of AkkaRPcService
	private static ActorSystem actorSystem = AkkaUtils.createDefaultActorSystem();

	private static AkkaRpcService akkaRpcService =
		new AkkaRpcService(actorSystem, new Timeout(10000, TimeUnit.MILLISECONDS));

	@AfterClass
	public static void shutdown() {
		akkaRpcService.stopService();
		actorSystem.shutdown();
	}

	@Test
	public void notifyOfNewResourceManagerLeader() throws Exception {
		Timeout akkaTimeout = new Timeout(10, TimeUnit.SECONDS);
		ActorSystem actorSystem = AkkaUtils.createDefaultActorSystem();
		ActorSystem actorSystem2 = AkkaUtils.createDefaultActorSystem();
		AkkaRpcService akkaRpcService = new AkkaRpcService(actorSystem, akkaTimeout);
		AkkaRpcService akkaRpcService2 = new AkkaRpcService(actorSystem2, akkaTimeout);
		ExecutorService executorService = new ForkJoinPool();

		ResourceManager resourceManager = new ResourceManager(akkaRpcService, executorService);
		JobGraph mockJob= Mockito.mock(JobGraph.class);
		HighAvailabilityServices haServices = new NonHaServices(resourceManager.getAddress());
		JobMaster jobMaster = new JobMaster(
			mockJob,
			haServices,
			akkaRpcService2,
			executorService);

		resourceManager.start();
		jobMaster.start();
		jobMaster.getSelf().notifyOfNewResourceManagerLeader(resourceManager.getAddress(), UUID.randomUUID());
		Mockito.doReturn(new JobID()).when(mockJob).getJobID();
		jobMaster.resetResourceManagerConnection();

		// wait for successful registration
		FiniteDuration timeout = new FiniteDuration(200, TimeUnit.SECONDS);
		Deadline deadline = timeout.fromNow();

		while (deadline.hasTimeLeft() && !jobMaster.isConnected()) {
			Thread.sleep(100);
		}

		assertFalse(deadline.isOverdue());
		jobMaster.shutDown();
		resourceManager.shutDown();
	}

}
