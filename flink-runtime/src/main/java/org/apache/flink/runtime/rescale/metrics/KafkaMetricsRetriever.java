package org.apache.flink.runtime.rescale.metrics;

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

import java.util.*;

import static org.apache.flink.util.Preconditions.checkState;

public class KafkaMetricsRetriever implements StreamSwitchMetricsRetriever {
	private static final Logger LOG = LoggerFactory.getLogger(KafkaMetricsRetriever.class);
	private String servers;

	private String TOPIC;
	private KafkaConsumer<String, String> consumer;
	private JobGraph jobGraph;
	private JobVertexID vertexID;
	private List<JobVertexID> upstreamVertexIDs = new ArrayList<>();

	int numInstances = 0;

	private final HashMap<String, Double> inputRate = new HashMap<>();
	private final HashMap<String, Double> trueProcessingRate = new HashMap<>();



	@Override
	public void init(JobGraph jobGraph, JobVertexID vertexID, Configuration jobConfiguration, int numPartitions, int parallelism) {
		this.jobGraph = jobGraph;
		this.vertexID = vertexID;

		for (JobVertex jobVertex : jobGraph.getVertices()) {
			numInstances += jobVertex.getParallelism();
		}

		LOG.info("++++++ initial number of instances: " + numInstances);

		TOPIC = jobConfiguration.getString("policy.metrics.topic", "flink_metrics");
		servers = jobConfiguration.getString("policy.metrics.servers", "localhost:9092");
		int nRecords = jobConfiguration.getInteger("model.retrieve.nrecords", 15);

		JobVertex curVertex = jobGraph.findVertexByID(vertexID);
		for (JobEdge jobEdge : curVertex.getInputs()) {
			JobVertexID id = jobEdge.getSource().getProducer().getID();
			upstreamVertexIDs.add(id);
		}

		initConsumer();
	}

	public void initConsumer() {
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, vertexID.toString());
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		consumer = new KafkaConsumer(props);
		consumer.subscribe(Arrays.asList(TOPIC));

		LOG.info("consumer object: " + consumer.hashCode());

		ConsumerRecords<String, String> records = consumer.poll(100);
		Set<TopicPartition> assignedPartitions = consumer.assignment();
		consumer.seekToEnd(assignedPartitions);
		for (TopicPartition partition : assignedPartitions) {
			long endPosition = consumer.position(partition);
			LOG.info("++++++ cur vertex: " + vertexID.toString() + " start offset: " + endPosition);
		}
	}

	@Override
	public Map<String, Object> retrieveMetrics() {
		synchronized (consumer) {
			ConsumerRecords<String, String> records = consumer.poll(100);
//			Set<TopicPartition> assignedPartitions = consumer.assignment();
//			for (TopicPartition partition : assignedPartitions) {
//				// move to the last offset of the topic in Kafka
//				long endPosition = consumer.position(partition);
//			}
			if (!records.isEmpty()) {
				// parse records, should construct metrics hashmap
				for (ConsumerRecord<String, String> record : records) {
					if (record.value().equals("")) {
						continue;
					}

					String[] ratesLine = record.value().split(",");
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
			}

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
