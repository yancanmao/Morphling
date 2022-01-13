package org.apache.flink.runtime.rescale.streamswitch;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rescale.controller.OperatorControllerListener;
import org.apache.flink.runtime.rescale.metrics.KafkaMetricsRetriever;
import org.apache.flink.runtime.rescale.metrics.PravegaMetricsRetriever;
import org.apache.flink.runtime.rescale.metrics.StockMetricsRetriever;
import org.apache.flink.runtime.rescale.metrics.StreamSwitchMetricsRetriever;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class MorphlingController extends Thread implements FlinkOperatorController {

	private static final Logger LOG = LoggerFactory.getLogger(MorphlingController.class);

	private final String name;

	private double conservativeFactor; // Initial prediction by user or system on service rate.

	private long metricsRetreiveInterval;

	long migrationInterval;


	private OperatorControllerListener listener;

	Map<String, List<String>> executorMapping;

	private volatile boolean waitForMigrationDeployed;

	private volatile boolean isStopped;

	private Random random;

	private boolean isStarted;

	private Configuration config;

	protected StreamSwitchMetricsRetriever metricsRetriever;

	private int numPartitions; // be used for metrics retriever, for initial metrics

	private String[] targetVertices;

	public MorphlingController(String name) {
		this.name = name;
	}

	public MorphlingController(Configuration config){
		name = config.getString("vertex_id", "c21234bcbf1e8eb4c61f1927190efebd"); // use jobvertex here
		metricsRetreiveInterval = config.getInteger("morphling.system.metrics_interval", 10000);
		conservativeFactor = config.getDouble("morphling.system.conservative", 1.0);
		migrationInterval = config.getLong("morphling.system.migration_interval", 5000l);
		isStarted = false;
	}


	@Override
	public void init(OperatorControllerListener listener, List<String> executors, List<String> partitions) {
		this.listener = listener;

		this.executorMapping = new HashMap<>();
		this.numPartitions = partitions.size();

		int numExecutors = executors.size();
		for (int executorId = 0; executorId < numExecutors; executorId++) {
			List<String> executorPartitions = new ArrayList<>();
			executorMapping.put(String.valueOf(executorId), executorPartitions);

			KeyGroupRange keyGroupRange = KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
				numPartitions, numExecutors, executorId);
			for (int i = keyGroupRange.getStartKeyGroup(); i <= keyGroupRange.getEndKeyGroup(); i++) {
				executorPartitions.add(String.valueOf(i));
			}
		}

		this.random = new Random();
		this.random.setSeed(System.currentTimeMillis());

		this.listener.setup(executorMapping);
	}

	@Override
	public void initMetrics(JobGraph jobGraph, JobVertexID vertexID, Configuration config, int parallelism) {
		String app = config.getString("model.app", "others");
		if (app.equals("stock")) {
			this.metricsRetriever = new StockMetricsRetriever();
		} else {
			this.metricsRetriever = new KafkaMetricsRetriever();
		}
		this.metricsRetriever.init(jobGraph, vertexID, config, numPartitions, parallelism);
		// TODO: init job configurations can be placed into constructor.
		this.config = config;
		String verticesStr = config.getString("model.vertex", "c21234bcbf1e8eb4c61f1927190efebd");
		targetVertices = verticesStr.split(",");
		for (int i = 0; i < targetVertices.length; i++) {
			targetVertices[i] = targetVertices[i].trim();
		}
		migrationInterval = config.getLong("streamswitch.system.migration_interval", 5000l); //Scale-out takes some time
	}

	@Override
	public void onForceRetrieveMetrics() {
	}

	@Override
	public void stopGracefully() {
		isStopped = true;
	}

	@Override
	public void onMigrationExecutorsStopped() {

	}

	@Override
	public void onMigrationCompleted() {
		waitForMigrationDeployed = false;
	}

	@Override
	public void run() {
		// check if need to start the monitoring for the operator
		boolean isContain = false;
		for (String targetVertex : targetVertices) {
			if (metricsRetriever.getVertexId().equals(JobVertexID.fromHexString(targetVertex))) {
				isContain = true;
			}
		}
		if (!isContain) return;
		try {
			LOG.info("------ " + name + " start to run");
			// cool down time, wait for fully deployment
			Thread.sleep(5 * 1000);
			while(!isInterrupted()) {
				Map<String, Object> metrics = metricsRetriever.retrieveMetrics();
				// retrieve metrics and evaluate based on the model
				// model: sum input rate / avg true processing rate
				// if the new parallelism is much different from the old parallelism, scaling to the target parallelism.
				if (metrics == null) continue;
				int parallelism = (int) metrics.get(targetVertices[0]);
				LOG.info("++++++ expected parallelism: " + parallelism);
				// scaling when the parallelism changed
				if (parallelism > executorMapping.size()) {
					LOG.info("++++++ invoke scaling from " + executorMapping.size() + " to: " + parallelism);
					preparePartitionAssignment(parallelism);
					triggerAction(
						"trigger scale out",
						() -> listener.scale(parallelism, executorMapping),
						executorMapping);
				}
				Thread.sleep(metricsRetreiveInterval);
			}
			LOG.info("------ " + name + " finished");
		} catch (Exception e) {
			LOG.info("------ " + name + " exception", e);
		}
	}



	private void triggerAction(String logStr, Runnable runnable, Map<String, List<String>> partitionAssignment) throws InterruptedException {
		LOG.info("------ " + name + "  " + logStr + "   partitionAssignment: " + partitionAssignment);
		waitForMigrationDeployed = true;

		runnable.run();

		while (waitForMigrationDeployed);

		// clear past metrics retrieved
		metricsRetriever.clear();
	}

	private void preparePartitionAssignment(int parallelism) {
		executorMapping.clear();

		for (int operatorIndex = 0; operatorIndex < parallelism; operatorIndex++) {
			int start = ((operatorIndex * numPartitions + parallelism - 1) / parallelism);
			int end = ((operatorIndex + 1) * numPartitions - 1) / parallelism;
			executorMapping.put(String.valueOf(operatorIndex), new ArrayList<>());
			for (int i = start; i <= end; i++) {
				executorMapping.get(String.valueOf(operatorIndex)).add(i + "");
			}
		}
	}

}
