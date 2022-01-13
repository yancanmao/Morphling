package org.apache.flink.runtime.rescale.metrics;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.*;

import static org.apache.flink.util.Preconditions.checkState;

public class PravegaMetricsRetriever implements StreamSwitchMetricsRetriever {
	private static final Logger LOG = LoggerFactory.getLogger(PravegaMetricsRetriever.class);

	private JobGraph jobGraph;
	private JobVertexID vertexID;
	private List<JobVertexID> upstreamVertexIDs = new ArrayList<>();

	int numInstances = 0;

	private final HashMap<String, Double> inputRate = new HashMap<>();
	private final HashMap<String, Double> trueProcessingRate = new HashMap<>();

	public String scope;
	public String streamName;
	public URI controllerURI;

	EventStreamReader<String> reader;

	@Override
	public void init(JobGraph jobGraph, JobVertexID vertexID, Configuration jobConfiguration, int numPartitions, int parallelism) {
		this.jobGraph = jobGraph;
		this.vertexID = vertexID;

		for (JobVertex jobVertex : jobGraph.getVertices()) {
			numInstances += jobVertex.getParallelism();
		}

		LOG.info("++++++ initial number of instances: " + numInstances);

		JobVertex curVertex = jobGraph.findVertexByID(vertexID);
		for (JobEdge jobEdge : curVertex.getInputs()) {
			JobVertexID id = jobEdge.getSource().getProducer().getID();
			upstreamVertexIDs.add(id);
		}

//		TOPIC = jobConfiguration.getString("policy.metrics.topic", "flink_metrics");
//		servers = jobConfiguration.getString("policy.metrics.servers", "localhost:9092");
//
//		initConsumer();

		scope = jobConfiguration.getString("policy.metrics.scope", "morphling");
		streamName = jobConfiguration.getString("policy.metrics.streamName", "flink_metrics");
		String uriString = jobConfiguration.getString("policy.metrics.controllerURI", "tcp://127.0.0.1:9090");
		controllerURI = URI.create(uriString);

		initPravegaConsumer();
	}

	private void initPravegaConsumer() {
		// create scope and stream
		StreamManager streamManager = StreamManager.create(controllerURI);

		final boolean scopeIsNew = streamManager.createScope(scope);
		StreamConfiguration streamConfig = StreamConfiguration.builder()
			.scalingPolicy(ScalingPolicy.fixed(1))
			.build();
		final boolean streamIsNew = streamManager.createStream(scope, streamName, streamConfig);

		final String readerGroup = UUID.randomUUID().toString().replace("-", "");

		final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
			.stream(Stream.of(scope, streamName))
			.build();
		try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI)) {
			readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
		}

		// create a reader
		EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope,
			ClientConfig.builder().controllerURI(controllerURI).build());
	 	reader = clientFactory.createReader("reader",
			 readerGroup,
			 new UTF8StringSerializer(),
			 ReaderConfig.builder().build());
	}


	@Override
	public Map<String, Object> retrieveMetrics() {
		EventRead<String> event = null;
		do {
			event = reader.readNextEvent(2000);
			if (event.getEvent() != null) {
				// parse records, should construct metrics hashmap
					if (event.getEvent().equals("")) {
						continue;
					}

					String[] ratesLine = event.getEvent().split(",");
					JobVertexID jobVertexId = JobVertexID.fromHexString(ratesLine[0]);
					// calculate inputRate and trueProcessingRate
					if (jobVertexId.equals(this.vertexID)) {
						// calculate true processing rate
						trueProcessingRate.put(ratesLine[1], Double.valueOf(ratesLine[4]));
					}
					if (upstreamVertexIDs.contains(jobVertexId)) {
						// calculate input rate
						inputRate.put(ratesLine[1], Double.valueOf(ratesLine[5]));
					}
				}
		} while (event.getEvent() != null);

		boolean isEpochFinished = true;
		// check whether the metrics in the current epoch has arrived
		checkState(trueProcessingRate.size() <= jobGraph.findVertexByID(this.vertexID).getParallelism());
		if (trueProcessingRate.size() < jobGraph.findVertexByID(this.vertexID).getParallelism()) {
			isEpochFinished = false;
		}
		checkState(inputRate.size() <= jobGraph.findVertexByID(upstreamVertexIDs.get(0)).getParallelism());
		if (inputRate.size() < jobGraph.findVertexByID(upstreamVertexIDs.get(0)).getParallelism()) {
			isEpochFinished = false;
		}


		Map<String, Object> metrics = new HashMap<>();
		if (isEpochFinished) {
			// return expected parallelism
			double totalInputRate = 0;
			for (double rate : inputRate.values()) {
				totalInputRate += rate;
			}
			double avgTrueProcessingRate = 0;
			for (double rate : trueProcessingRate.values()) {
				avgTrueProcessingRate += rate;
			}
			avgTrueProcessingRate /= trueProcessingRate.size();
			int parallelism = (int) Math.ceil(totalInputRate * 1.2 / avgTrueProcessingRate);
			metrics.put(this.vertexID.toString(), parallelism);
			trueProcessingRate.clear();
			inputRate.clear();
			return metrics;
		} else {
			return null;
		}
	}

	public JobVertexID getVertexId() {
		return vertexID;
	}

	@Override
	public void clear() {
		inputRate.clear();
		trueProcessingRate.clear();
	}
}
