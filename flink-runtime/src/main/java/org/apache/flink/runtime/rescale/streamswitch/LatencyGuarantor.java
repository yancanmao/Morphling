package org.apache.flink.runtime.rescale.streamswitch;

//import javafx.util.Pair;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rescale.controller.OperatorControllerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

//Under development

public class LatencyGuarantor extends StreamSwitch {
	private static final Logger LOG = LoggerFactory.getLogger(LatencyGuarantor.class);
	private long latencyReq, windowReq; //Window requirment is stored as number of timeslot
	private double l_low, l_high; // Check instantDelay  < l and longtermDelay < req
	private double initialServiceRate, decayFactor, conservativeFactor; // Initial prediction by user or system on service rate.
	long migrationInterval;
	private boolean isStarted;
	private Prescription pendingPres;
	private Examiner examiner;
	private Boolean isTreat, isFreshed;
	private String jobid;


	public LatencyGuarantor(Configuration config){
		jobid = config.getString("vertex_id", "c21234bcbf1e8eb4c61f1927190efebd"); // use jobvertex here
		isTreat = config.getBoolean("streamswitch.system.is_treat", true);
		metricsRetreiveInterval = config.getInteger("streamswitch.system.metrics_interval", 100);
		maxNumberOfExecutors = config.getInteger("streamswitch.system.max_executors", 64);
//		latencyReq = config.getLong("streamswitch.requirement.latency", 1000); //Unit: millisecond
		latencyReq = config.getLong("streamswitch.requirement.latency." + jobid, 1000); //Unit: millisecond
		windowReq = config.getLong("streamswitch.requirement.window", 2000) / metricsRetreiveInterval; //Unit: # of time slots
		l_low = config.getDouble("streamswitch.system.l_low", 50); //Unit: millisecond
		l_high = config.getDouble("streamswtich.system.l_high", 100);
		initialServiceRate = config.getDouble("streamswitch.system.initialservicerate", 0.2);
		decayFactor = config.getDouble("streamswitch.system.decayfactor", 0.875);
		conservativeFactor = config.getDouble("streamswitch.system.conservative", 1.0);
		migrationInterval = config.getLong("streamswitch.system.migration_interval", 5000l);
		examiner = new Examiner();
		pendingPres = null;
		isStarted = false;
		isFreshed = false;
	}

	@Override
	public void init(OperatorControllerListener listener, List<String> executors, List<String> substreams) {
		super.init(listener, executors, substreams);
		examiner.init(executorMapping);
	}
	class Examiner{
		class State {
			private Map<Long, Map<Integer, List<Integer>>> mappings;
			class SubstreamState{
				private Map<Long, Long> arrived, completed; //Instead of actual time, use the n-th time point as key
				private long lastValidIndex;    //Last valid state time point
				//For calculate instant delay
				private Map<Long, Long> totalLatency;
				private long arrivedIndex, remainedIndex;
				SubstreamState(){
					arrived = new HashMap<>();
					completed = new HashMap<>();
					totalLatency = new HashMap<>();
				}
			}
			Map<Integer, SubstreamState> substreamStates;
			private long lastValidTimeIndex;
			private long currentTimeIndex;
			private State() {
				currentTimeIndex = 0;
				lastValidTimeIndex = 0;
				mappings = new HashMap<>();
				substreamStates = new HashMap<>();
			}

			private int substreamIdFromStringToInt(String partition){
				return Integer.parseInt(partition.substring(partition.indexOf(' ') + 1));
			}
			private int executorIdFromStringToInt(String executor){
				return Integer.parseInt(executor);
			}

			private void init(Map<String, List<String>> executorMapping){
				LOG.info("Initialize time point 0...");
				System.out.println("New mapping at time: " + 0 + " mapping:" + executorMapping);
				for (String executor : executorMapping.keySet()) {
					for (String substream : executorMapping.get(executor)) {
						SubstreamState substreamState = new SubstreamState();
						substreamState.arrived.put(0l, 0l);
						substreamState.completed.put(0l, 0l);
						substreamState.lastValidIndex = 0l;
						substreamState.arrivedIndex = 1l;
						substreamState.remainedIndex = 0l;
						substreamStates.put(substreamIdFromStringToInt(substream), substreamState);
					}
				}
			}

			protected long getTimepoint(long timeIndex){
				return timeIndex * metricsRetreiveInterval;
			}

			protected Map<Integer, List<Integer>> getMapping(long n){
				return mappings.getOrDefault(n, null);
			}
			public long getSubstreamArrived(Integer substream, long n){
				long arrived = 0;
				if(substreamStates.containsKey(substream)){
					arrived = substreamStates.get(substream).arrived.getOrDefault(n, 0l);
				}
				return arrived;
			}

			public long getSubstreamCompleted(Integer substream, long n){
				long completed = 0;
				if(substreamStates.containsKey(substream)){
					completed = substreamStates.get(substream).completed.getOrDefault(n, 0l);
				}
				return completed;
			}


			public long getSubstreamLatency(Integer substream, long timeIndex){
				long latency = 0;
				if(substreamStates.containsKey(substream)){
					latency = substreamStates.get(substream).totalLatency.getOrDefault(timeIndex, 0l);
				}
				return latency;
			}

			//Calculate the total latency in new slot. Drop useless data during calculation.
			private void calculateSubstreamLatency(Integer substream, long timeIndex){
				//Calculate total latency and # of tuples in current time slots
				SubstreamState substreamState = substreamStates.get(substream);
				long arrivalIndex = substreamState.arrivedIndex;
				long complete = getSubstreamCompleted(substream, timeIndex);
				long lastComplete = getSubstreamCompleted(substream, timeIndex - 1);
				long arrived = getSubstreamArrived(substream, arrivalIndex);
				long lastArrived = getSubstreamArrived(substream, arrivalIndex - 1);
				long totalDelay = 0;
				while(arrivalIndex <= timeIndex && lastArrived < complete){
					if(arrived > lastComplete){ //Should count into this slot
						long number = Math.min(complete, arrived) - Math.max(lastComplete, lastArrived);
						totalDelay += number * (timeIndex - arrivalIndex);
					}
					arrivalIndex++;
					arrived = getSubstreamArrived(substream, arrivalIndex);
					lastArrived = getSubstreamArrived(substream, arrivalIndex - 1);
				}
				//TODO: find out when to --
				arrivalIndex--;
				substreamState.totalLatency.put(timeIndex, totalDelay);
				if(substreamState.totalLatency.containsKey(timeIndex - windowReq)){
					substreamState.totalLatency.remove(timeIndex - windowReq);
				}
				if(arrivalIndex > substreamState.arrivedIndex) {
					substreamState.arrivedIndex = arrivalIndex;
				}
			}

			private void drop(long timeIndex, Map<String, Boolean> substreamValid) {
				long totalSize = 0;

				//Drop arrived
				for(String substream: substreamValid.keySet()) {
					SubstreamState substreamState = substreamStates.get(substreamIdFromStringToInt(substream));
					if (substreamValid.get(substream)) {
						long remainedIndex = substreamState.remainedIndex;
						while (remainedIndex < substreamState.arrivedIndex - 1 && remainedIndex < timeIndex - windowReq) {
							substreamState.arrived.remove(remainedIndex);
							remainedIndex++;
						}
						substreamState.remainedIndex = remainedIndex;
					}
				}
				//Debug
				for (int substream : substreamStates.keySet()) {
					SubstreamState substreamState = substreamStates.get(substream);
					totalSize += substreamState.arrivedIndex - substreamState.remainedIndex + 1;
				}

				//Drop completed, mappings. These are fixed window

				//Drop mappings
				List<Long> removeIndex = new LinkedList<>();
				for(long index:mappings.keySet()){
					if(index < timeIndex - windowReq - 1){
						removeIndex.add(index);
					}
				}
				for(long index:removeIndex){
					mappings.remove(index);
				}
				removeIndex.clear();
				if (checkValidity(substreamValid)) {
					for (long index = lastValidTimeIndex + 1; index <= timeIndex; index++) {
						for (String executor : executorMapping.keySet()) {
							for (String substream : executorMapping.get(executor)) {
								//Drop completed
								if (substreamStates.get(substreamIdFromStringToInt(substream)).completed.containsKey(index - windowReq - 1)) {
									substreamStates.get(substreamIdFromStringToInt(substream)).completed.remove(index - windowReq - 1);
								}
							}
						}

					}
					lastValidTimeIndex = timeIndex;
				}

				LOG.info("Useless state dropped, current arrived size: " + totalSize + " mapping size: " + mappings.size());
			}

			//Only called when time n is valid, also update substreamLastValid
			private void calibrateSubstream(int substream, long timeIndex){
				SubstreamState substreamState = substreamStates.get(substream);
				long n0 = substreamState.lastValidIndex;
				substreamState.lastValidIndex = timeIndex;
				if(n0 < timeIndex - 1) {
					//LOG.info("Calibrate state for " + substream + " from time=" + n0);
					long a0 = state.getSubstreamArrived(substream, n0);
					long c0 = state.getSubstreamCompleted(substream, n0);
					long a1 = state.getSubstreamArrived(substream, timeIndex);
					long c1 = state.getSubstreamCompleted(substream, timeIndex);
					for (long i = n0 + 1; i < timeIndex; i++) {
						long ai = (a1 - a0) * (i - n0) / (timeIndex - n0) + a0;
						long ci = (c1 - c0) * (i - n0) / (timeIndex - n0) + c0;
						substreamState.arrived.put(i, ai);
						substreamState.completed.put(i, ci);
					}
				}

				//Calculate total latency here
				for(long index = n0 + 1; index <= timeIndex; index++){
					calculateSubstreamLatency(substream, index);
				}
			}

			//Calibrate whole state including mappings and utilizations
			private void calibrate(Map<String, Boolean> substreamValid){
				//Calibrate mappings
				if(mappings.containsKey(currentTimeIndex)){
					for(long t = currentTimeIndex - 1; t >= 0 && t >= currentTimeIndex - windowReq - 1; t--){
						if(!mappings.containsKey(t)){
							mappings.put(t, mappings.get(currentTimeIndex));
						}else{
							break;
						}
					}
				}

				//Calibrate substreams' state
				if(substreamValid == null)return ;
				for(String substream: substreamValid.keySet()){
					if (substreamValid.get(substream)){
						calibrateSubstream(substreamIdFromStringToInt(substream), currentTimeIndex);
					}
				}
			}



			private void insert(long timeIndex, Map<String, Long> substreamArrived,
								Map<String, Long> substreamProcessed,
								Map<String, List<String>> executorMapping) { //Normal update
				LOG.info("Debugging, metrics retrieved data, time: " + timeIndex + " substreamArrived: "+ substreamArrived + " substreamProcessed: "+ substreamProcessed + " assignment: " + executorMapping);

				currentTimeIndex = timeIndex;

				HashMap<Integer, List<Integer>> mapping = new HashMap<>();
				for(String executor: executorMapping.keySet()) {
					List<Integer> partitions = new LinkedList<>();
					for (String partitionId : executorMapping.get(executor)) {
						partitions.add(substreamIdFromStringToInt(partitionId));
					}
					mapping.put(executorIdFromStringToInt(executor), partitions);
				}
				mappings.put(currentTimeIndex, mapping);

				LOG.info("Current time " + timeIndex);

				for(String substream:substreamArrived.keySet()){
					substreamStates.get(substreamIdFromStringToInt(substream)).arrived.put(currentTimeIndex, substreamArrived.get(substream));
				}
				for(String substream: substreamProcessed.keySet()){
					substreamStates.get(substreamIdFromStringToInt(substream)).completed.put(currentTimeIndex, substreamProcessed.get(substream));
				}

			}
		}
		//Model here is only a snapshot
		class Model {
			private State state;
			Map<String, Double> substreamArrivalRate, executorArrivalRate, executorServiceRate, executorInstantaneousDelay; //Longterm delay could be calculated from arrival rate and service rate
			Map<String, Double> executorBacklogDelay;
			Map<String, Long> executorBacklog, executorArrived;
			Map<String, Long> executorCompleted; //For debugging instant delay
			Set<String> invalidExecutors; // For multi-migrations
			private long maximumMigrationTime; // For self-adaptive migration time.
			private long maximumMigrationTimeWindow; // For sliding window of self-adaptive migration time
			private Deque<Map.Entry<Long, Long>> potentialMaximumMigrationTimes; // For sliding window of self-adaptive migration time
			private final boolean selfAdaptiveMigrationTimeFlag;
			private Map<String, Long> substreamLastRunningTime;
			private Map<String, Long> substreamLastDecisionTime; // Use negative integer to indicate before synchronization
			private Model(State state){
				substreamArrivalRate = new HashMap<>();
				executorArrivalRate = new HashMap<>();
				executorServiceRate = new HashMap<>();
				executorInstantaneousDelay = new HashMap<>();
				executorBacklog = new HashMap<>();
				executorArrived  = new HashMap<>();
				executorCompleted = new HashMap<>();
				invalidExecutors = new HashSet<>();
				maximumMigrationTime = config.getLong("streamswitch.system.maxmigrationtime", 500);
				selfAdaptiveMigrationTimeFlag = config.getBoolean("streamswitch.system.selfadaptivemigrationtime", false);
				maximumMigrationTimeWindow = config.getLong("streamswitch.system.maxmigrationtimewindow", 86400); // Unit: second, 1 day
				potentialMaximumMigrationTimes = new LinkedList<>();
				substreamLastRunningTime = new HashMap<>();
				substreamLastDecisionTime = new HashMap<>();
				this.state = state;
			}

			private void updateMaximumMigrationTime(long migrationTime, long time){
				while(!potentialMaximumMigrationTimes.isEmpty() && potentialMaximumMigrationTimes.peekFirst().getValue() < time){
					potentialMaximumMigrationTimes.pollFirst();
				}
				while(!potentialMaximumMigrationTimes.isEmpty() && potentialMaximumMigrationTimes.peekLast().getKey() <= migrationTime){
					potentialMaximumMigrationTimes.pollLast();
				}
				potentialMaximumMigrationTimes.addLast(new AbstractMap.SimpleEntry<Long, Long>(migrationTime, time + maximumMigrationTimeWindow * 1000));
				maximumMigrationTime = potentialMaximumMigrationTimes.getFirst().getKey();
			}

			private double calculateLongTermDelay(double arrival, double service){
				// Conservative !
				service = conservativeFactor * service;
				if(arrival < 1e-15)return 0.0;
				if(service < arrival + 1e-15)return 1e100;
				return 1.0/(service - arrival);
			}

			// 1 / ( u - n ). Return  1e100 if u <= n
			private double getLongTermDelay(String executorId){
				double arrival = executorArrivalRate.get(executorId);
				double service = executorServiceRate.get(executorId);
				return calculateLongTermDelay(arrival, service);
			}

			private double calculateSubstreamArrivalRate(String substream, long n0, long n1){
				if(n0 < 0)n0 = 0;
				long time = state.getTimepoint(n1) - state.getTimepoint(n0);
				long totalArrived = state.getSubstreamArrived(state.substreamIdFromStringToInt(substream), n1) - state.getSubstreamArrived(state.substreamIdFromStringToInt(substream), n0);
				if(time > 1e-9)return totalArrived / ((double)time);
				return 0.0;
			}

			private double calculateExecutorBacklogDelay(String executorId, long timeIndex){
				long totalBacklog = 0, totalArrived = 0;
				//long n0 = timeIndex - windowReq + 1;
				long n0 = timeIndex;
				if(n0<1){
					n0 = 1;
					LOG.warn("Calculate instant delay index smaller than window size!");
				}
				for(long i = n0; i <= timeIndex; i++) {
					if (state.getMapping(i).containsKey(state.executorIdFromStringToInt(executorId))) {
						for (int substream : state.getMapping(i).get(state.executorIdFromStringToInt(executorId))) {
							totalArrived += state.getSubstreamArrived(substream, i);
							totalBacklog += state.getSubstreamArrived(substream, i) - state.getSubstreamCompleted(substream, i);

						}
					}
				}
				executorBacklog.put(executorId, totalBacklog);
				executorArrived.put(executorId, totalArrived);
				if( (timeIndex - n0 + 1) > 0 && executorServiceRate.getOrDefault(executorId, 0.0) > 0.0)return totalBacklog / (executorServiceRate.get(executorId) * (timeIndex - n0 + 1));
				return 1e100;
			}

			//Window average delay
			private double calculateExecutorInstantaneousDelay(String executorId, long timeIndex){
				long totalDelay = 0;
				long totalCompleted = 0;
				long n0 = timeIndex - windowReq + 1;
				if(n0<1){
					n0 = 1;
					LOG.warn("Calculate instant delay index smaller than window size!");
				}
				for(long i = n0; i <= timeIndex; i++){
					if(state.getMapping(i).containsKey(state.executorIdFromStringToInt(executorId))){
						for(int substream: state.getMapping(i).get(state.executorIdFromStringToInt(executorId))){
							totalCompleted += state.getSubstreamCompleted(substream, i) - state.getSubstreamCompleted(substream, i - 1);
							totalDelay += state.getSubstreamLatency(substream, i);
						}
					}
				}
				//In state, latency is count as # of timeslots, need to transfer to real time
				executorCompleted.put(executorId, totalCompleted);
				if(totalCompleted > 0) return totalDelay * metricsRetreiveInterval / ((double)totalCompleted);
				return 0;
			}

			//Calculate model snapshot from state
			private void update(final long timeIndex, Map<String, Double> serviceRate, Map<String, Boolean> substreamValidity, Map<String, List<String>> executorMapping){
				LOG.info("Updating model snapshot, clear old data...");
				//substreamArrivalRate.clear();
				executorArrivalRate.clear();
				//executorInstantaneousDelay.clear();
				executorCompleted.clear();
				invalidExecutors.clear();
				Map<String, Double> utils = new HashMap<>();
				for(String executor: executorMapping.keySet()){
					double arrivalRate = 0;
					boolean isValid = true;
					for(String substream: executorMapping.get(executor)){
						if(substreamValidity != null && substreamValidity.getOrDefault(substream, false)) {
							double oldArrivalRate = substreamArrivalRate.getOrDefault(substream, 0.0);
							double t = oldArrivalRate * decayFactor + calculateSubstreamArrivalRate(substream, timeIndex - windowReq, timeIndex) * (1.0 - decayFactor);
							substreamArrivalRate.put(substream, t);
							arrivalRate += t;
						}else{
							isValid = false;
						}
					}
					if(isValid) {
						executorArrivalRate.put(executor, arrivalRate);
						//double util = state.getExecutorUtilization(state.executorIdFromStringToInt(executor));
						//utils.put(executor, util);
						//double mu = calculateExecutorServiceRate(executor, util, timeIndex);
                    /*if(util > 0.5 && util <= 1){ //Only update true service rate (capacity when utilization > 50%, so the error will be smaller)
                        mu /= util;
                        executorServiceRate.put(executor, mu);
                    }else if(!executorServiceRate.containsKey(executor) || (util < 0.3 && executorServiceRate.get(executor) < arrivalRate * 1.5))executorServiceRate.put(executor, arrivalRate * 1.5); //Only calculate the service rate when no historical service rate*/

						//executorServiceRate.put(executor, mu);

						if (!executorServiceRate.containsKey(executor)) {
							executorServiceRate.put(executor, initialServiceRate);
						}
						if (serviceRate.containsKey(executor)) {
							double oldServiceRate = executorServiceRate.get(executor);
							double newServiceRate = oldServiceRate * decayFactor + serviceRate.get(executor) * (1 - decayFactor);
							executorServiceRate.put(executor, newServiceRate);
						}

						double oldInstantaneousDelay = executorInstantaneousDelay.getOrDefault(executor, 0.0);
						double newInstantaneousDelay = oldInstantaneousDelay * decayFactor + calculateExecutorInstantaneousDelay(executor, timeIndex) * (1.0 - decayFactor);
						executorInstantaneousDelay.put(executor, newInstantaneousDelay);
						double newBacklogDelay = calculateExecutorBacklogDelay(executor, timeIndex);
						executorBacklogDelay.put(executor, newBacklogDelay);
					}else{
						invalidExecutors.add(executor);
					}
				}

				// Update self-adaptive maximum migration time
				if (selfAdaptiveMigrationTimeFlag){
					for(String executor: executorMapping.keySet()){
						for(String substream: executorMapping.get(executor)){
							long tTimeIndex = substreamLastRunningTime.getOrDefault(substream, 0l);
							updateMaximumMigrationTime(state.getTimepoint(timeIndex) - state.getTimepoint(tTimeIndex), state.getTimepoint(timeIndex));

							if(substreamLastDecisionTime.containsKey(substream)){
								tTimeIndex = substreamLastDecisionTime.get(substream);
								if(tTimeIndex > 0){
									updateMaximumMigrationTime(state.getTimepoint(timeIndex) - state.getTimepoint(tTimeIndex), state.getTimepoint(timeIndex));
								}
							}
							// Consider the time between decision to completed instead of only validity.
							if(substreamValidity.getOrDefault(substream, false) &&
								timeIndex > 0){
								tTimeIndex = substreamLastRunningTime.getOrDefault(substream, 0l);
								long oldArrived = state.getSubstreamArrived(state.substreamIdFromStringToInt(substream), tTimeIndex);
								long oldCompleted = state.getSubstreamCompleted(state.substreamIdFromStringToInt(substream), tTimeIndex);
								long newCompleted = state.getSubstreamCompleted(state.substreamIdFromStringToInt(substream), timeIndex);
								if(newCompleted > oldCompleted || oldArrived == oldCompleted) {
									substreamLastRunningTime.put(substream, timeIndex);
									if (substreamLastDecisionTime.containsKey(substream) && substreamLastDecisionTime.get(substream) > 0) {
										substreamLastDecisionTime.remove(substream);
									}
								}
							}
						}
					}
					LOG.info("Debugging, maximumMigrationTime: " + maximumMigrationTime);
				}


				//Debugging
				LOG.info("Debugging, avg utilization: " + utils);
				LOG.info("Debugging, partition arrival rate: " + substreamArrivalRate);
				LOG.info("Debugging, executor avg service rate: " + executorServiceRate);
				LOG.info("Debugging, executor avg delay: " + executorInstantaneousDelay);
			}
		}

		private Model model;
		private State state;
		Examiner(){
			state = new State();
			model = new Model(state);
		}

		private void init(Map<String, List<String>> executorMapping){
			state.init(executorMapping);
		}

		private boolean checkValidity(Map<String, Boolean> substreamValid){
			if(substreamValid == null)return false;

			//Current we don't haven enough time slot to calculate model
			if(state.currentTimeIndex < windowReq){
				LOG.info("Current time slots number is smaller than beta, not valid");
				return false;
			}
			//Substream Metrics Valid
			for(String executor: executorMapping.keySet()) {
				for (String substream : executorMapping.get(executor)) {
					if (!substreamValid.containsKey(substream) || !substreamValid.get(substream)) {
						LOG.info(substream + "'s metrics is not valid");
						return false;
					}
				}
			}

			return true;
		}

		private boolean updateState(long timeIndex, Map<String, Long> substreamArrived, Map<String, Long> substreamProcessed, Map<String, Boolean> substreamValid, Map<String, List<String>> executorMapping){
			LOG.info("Updating state...");
			state.insert(timeIndex, substreamArrived, substreamProcessed, executorMapping);
			state.calibrate(substreamValid);
			state.drop(timeIndex, substreamValid);
			//Debug & Statistics
			HashMap<Integer, Long> arrived = new HashMap<>(), completed = new HashMap<>();
			for(int substream: state.substreamStates.keySet()) {
				arrived.put(substream, state.substreamStates.get(substream).arrived.get(state.currentTimeIndex));
				completed.put(substream, state.substreamStates.get(substream).completed.get(state.currentTimeIndex));
			}
			System.out.println("State, time " + timeIndex + " , jobid: " + jobid + " , Partition Arrived: " + arrived);
			System.out.println("State, time " + timeIndex + " , jobid: " + jobid + " , Partition Completed: " + completed);
			if(checkValidity(substreamValid)){
				//state.calculate(timeIndex);
				//state.drop(timeIndex);
				return true;
			}else return false;
		}

		private void updateModel(long timeIndex, Map<String, Double> serviceRate, Map<String, Boolean> substreamValid, Map<String, List<String>> executorMapping){
			LOG.info("Updating Model");
			model.update(timeIndex, serviceRate, substreamValid, executorMapping);;
			//Debug & Statistics
			if(true){
				HashMap<String, Double> longtermDelay = new HashMap<>();
				for(String executorId: executorMapping.keySet()){
					double delay = model.getLongTermDelay(executorId);
					longtermDelay.put(executorId, delay);
				}
				System.out.println("Model, time " + timeIndex + " , jobid: " + jobid + " , Arrival Rate: " + model.executorArrivalRate);
				System.out.println("Model, time " + timeIndex + " , jobid: " + jobid + " , Service Rate: " + model.executorServiceRate);
				System.out.println("Model, time " + timeIndex + " , jobid: " + jobid + " , Instantaneous Delay: " + model.executorInstantaneousDelay);
				System.out.println("Model, time " + timeIndex + " , jobid: " + jobid + " , executors completed: " + model.executorCompleted);
				System.out.println("Model, time " + timeIndex + " , jobid: " + jobid + " , Longterm Delay: " + longtermDelay);
				System.out.println("Model, time " + timeIndex + " , jobid: " + jobid + " , Partition Arrival Rate: " + model.substreamArrivalRate);
				System.out.println("Model, time " + timeIndex  + " , Arrived: " + model.executorArrived);
				System.out.println("Model, time " + timeIndex  + " , Backlog: " + model.executorBacklog);
				System.out.println("Model, time " + timeIndex  + " , Backlog Delay: " + model.executorBacklogDelay);
				if(model.selfAdaptiveMigrationTimeFlag){
					System.out.println("Model, time " + timeIndex  + " , MaxMigrationTime: " + model.maximumMigrationTime + " , " + model.potentialMaximumMigrationTimes.peekFirst().getValue());
				}
			}
		}
		private Map<String, Double> getInstantDelay(){
			return model.executorInstantaneousDelay;
		}

		private Map<String, Double> getBacklogDelay(){
			return model.executorBacklogDelay;
		}

		private Map<String, Double> getLongtermDelay(){
			HashMap<String, Double> delay = new HashMap<>();
			for(String executor: executorMapping.keySet()){
				delay.put(executor, model.getLongTermDelay(executor));
			}
			return delay;
		}
	}
	class Prescription {
		List<String> sources, targets;
		Map<String, Map.Entry<String, String>> migratingSubstreams;
		Prescription(){
			migratingSubstreams = null;
		}
		Prescription(List<String> sources, List<String> targets, Map<String, Map.Entry<String, String>> migratingSubstreams){
			this.sources = sources;
			this.targets = targets;
			this.migratingSubstreams = migratingSubstreams;
		}
		Map<String, List<String>> generateNewSubstreamAssignment(Map<String, List<String>> oldAssignment){
			Map<String, List<String>> newAssignment = new HashMap<>();
			for(String executor: oldAssignment.keySet()){
				List<String> substreams = new ArrayList<>(oldAssignment.get(executor));
				newAssignment.put(executor, substreams);
			}
			for(String target: targets) {
				if (!newAssignment.containsKey(target)) newAssignment.put(target, new LinkedList<>());
			}
			for (Map.Entry<String, Map.Entry<String, String>> substream : migratingSubstreams.entrySet()) {
				String source = substream.getValue().getKey(), target = substream.getValue().getValue();
				newAssignment.get(source).remove(substream.getKey());
				newAssignment.get(target).add(substream.getKey());
			}
			//For scale in
			for(String source: sources) {
				if (newAssignment.get(source).size() == 0) newAssignment.remove(source);
			}return newAssignment;
		}
	}
	private Prescription diagnose(Examiner examiner){

		class Diagnoser { //Algorithms and utility methods
			//Get severe executor with largest longterm latency.
			private Pair<String, Double> getWorstExecutor(Set<String> oes){
				double initialDelay = -1.0;
				String maxExecutor = "";
				for (String executor : oes) {
					double longtermDelay = examiner.model.getLongTermDelay(executor);
					double instantDelay = examiner.model.executorInstantaneousDelay.get(executor);
					if(instantDelay > l_high && longtermDelay > latencyReq) {
						if (longtermDelay > initialDelay) {
							initialDelay = longtermDelay;
							maxExecutor = executor;
						}
					}
				}
				return new Pair(maxExecutor, initialDelay);
			}
			//Compare two delay vector, assume they are sorted
			private boolean vectorGreaterThan(List<Double> v1, List<Double> v2){
				Iterator<Double> i1 = v1.iterator(), i2 = v2.iterator();
				while(i1.hasNext()){
					double t1 = i1.next(), t2 = i2.next();
					if(t1 - 1e-9 > t2)return true;
					if(t2 - 1e-9 > t1)return false;
				}
				return false;
			}
			//Calculate healthisness of input delay vectors: 0 for Good, 1 for Moderate, 2 for Severe
			private int getHealthiness(Map<String, Double> instantDelay, Map<String, Double> longtermDelay, Set<String> oes){
				boolean isGood = true;
				for(String oe: oes){
					double L = longtermDelay.get(oe);
					double l = instantDelay.get(oe);
					if(L > latencyReq){
						if(l > l_high)return SEVERE; //Only consider unlocked OEs to scale out/LB
						isGood = false;
					}
					if(l > l_low)isGood = false;
				}
				if(isGood)return GOOD;
				else return MODERATE;
			}

			//Debug
			private int countSevereExecutors(Map<String, Double> instantDelay, Map<String, Double> longtermDelay, Set<String> oes){
				int numberOfSevere = 0;
				for(String oe: oes){
					double L = longtermDelay.get(oe);
					double l = instantDelay.get(oe);
					if(L > latencyReq && l > l_high){
						numberOfSevere ++;
					}
				}
				return numberOfSevere;
			}

			// Find the subset which minimizes delay by greedy:
			// Sort arrival rate from largest to smallest, cut them in somewhere
			/* private Pair<Prescription, Map<String, Double>> scaleOut(Set<String> oes){
				LOG.info("Scale out by one container");
				if(oes.size() <= 0){
					LOG.info("No executor to move");
					return new Pair<Prescription, Map<String, Double>>(new Prescription(), null);
				}
				if(executorMapping.size() + 1 > maxNumberOfExecutors){
					LOG.info("Reach executor number limit, cannot scale out");
					return new Pair<>(new Prescription(), null);
				}

				Pair<String, Double> a = getWorstExecutor(oes);
				String srcExecutor = a.getKey();
				if(srcExecutor == null || srcExecutor.equals("") || executorMapping.get(srcExecutor).size() <=1){
					LOG.info("Cannot scale out: insufficient substream to migrate");
					return new Pair<Prescription, Map<String, Double>>(new Prescription(), null);
				}

				LOG.info("Migrating out from executor " + srcExecutor);

				//Try from smallest latency to largest latency
				TreeMap<Double, Set<String>> substreams = new TreeMap<>();

				for(String substream: executorMapping.get(srcExecutor)){
					double delay = examiner.state.getSubstreamLatency(examiner.state.substreamIdFromStringToInt(substream), examiner.state.currentTimeIndex) /
						((double) examiner.state.getSubstreamCompleted(examiner.state.substreamIdFromStringToInt(substream), examiner.state.currentTimeIndex) - examiner.state.getSubstreamCompleted(examiner.state.substreamIdFromStringToInt(substream), examiner.state.currentTimeIndex - 1));
					substreams.putIfAbsent(delay, new HashSet<>());
					substreams.get(delay).add(substream);
				}
				//Debugging
				LOG.info("Debugging, substreams' latency=" + substreams);

				List<String> sortedSubstreams = new LinkedList<>();
				for(Map.Entry<Double, Set<String>> entry: substreams.entrySet()){
					sortedSubstreams.addAll(entry.getValue());
				}

				double arrivalrate0 = examiner.model.executorArrivalRate.get(srcExecutor), arrivalrate1 = 0;
				//double executorServiceRate = examiner.model.executorServiceRate.get(srcExecutor); //Assume new executor has same service rate
				double best = 1e100;
				List<String> migratingSubstreams = new ArrayList<>();
				for(String substream: sortedSubstreams){
					double arrival = examiner.model.substreamArrivalRate.get(substream);
					arrivalrate0 -= arrival;
					arrivalrate1 += arrival;

					if(Math.max(arrivalrate0, arrivalrate1) < best){
						best = Math.max(arrivalrate0, arrivalrate1);
						migratingSubstreams.add(substream);
					}
					if(arrivalrate0 < arrivalrate1)break;
				}
				long newExecutorId = nextExecutorID.get();
				String tgtExecutor = String.valueOf(newExecutorId);
				if(newExecutorId + 1 > nextExecutorID.get()){
					nextExecutorID.set(newExecutorId + 1);
				}
				//LOG.info("Debugging, scale out migrating substreams: " + migratingSubstreams);
				return new Pair<Prescription, Map<String, Double>>(new Prescription(srcExecutor, tgtExecutor, migratingSubstreams), null);
			} */

			private boolean isExecutorSevere(long backlog, double serviceRate, double arrivalRate, long migrationTime){
				return (backlog + arrivalRate * migrationTime) / (serviceRate * conservativeFactor) > latencyReq && arrivalRate > serviceRate * conservativeFactor;
			}

			private boolean isExecutorSafe(long backlog, double serviceRate, double arrivalRate, long migrationTime){
				return (backlog + arrivalRate * migrationTime) / (serviceRate * conservativeFactor) < latencyReq && arrivalRate < serviceRate * conservativeFactor;
			}

			//Find severe OEs: (b + lambda * Tm)/ mu > L && arrivalRate > serviceRate
			private Set<String> findSevereOEs(Set<String> activatedOEs, long migrationTime){
				LOG.info("Finding severe oes");
				Set<String> severeOEs = new HashSet<>();
				for(String oe: activatedOEs){
					long backlog = examiner.model.executorBacklog.get(oe);
					double serviceRate = examiner.model.executorServiceRate.get(oe);
					double arrivalRate = examiner.model.executorArrivalRate.get(oe);
					if(isExecutorSevere(backlog, serviceRate, arrivalRate, migrationTime)){
						severeOEs.add(oe);
					}
				}
				LOG.info("Severe oes:" + severeOEs);
				return severeOEs;
			}

			// Check whether all OEs are safe: b/mu + Tm < L & lambda < mu
			private boolean checkSafe(Set<String> activatedOEs, long migrationTime){
				LOG.info("Check whether all OEs are safe.");
				boolean safe = true;
				for(String oe: activatedOEs){
					//double backlogDelay = examiner.model.executorBacklogDelay.get(oe);
					long backlog = examiner.model.executorBacklog.get(oe);
					double serviceRate = examiner.model.executorServiceRate.get(oe);
					double arrivalRate = examiner.model.executorArrivalRate.get(oe);
					if(!isExecutorSafe(backlog, serviceRate, arrivalRate, migrationTime)){
						LOG.info(oe + " is not safe.");
						safe = false;
					}
				}
				if(!safe)return false;
				LOG.info("All OEs are safe.");
				return true;
			}

			// Migrate substreams out from severe OEs to make them not caution.
			private Pair<Prescription, Map<String, Double>> loadBalanceAndScaleOut(Set<String> activatedOEs, Set<String> severeOEs, long migrationTime){
				LOG.info("Scale out from severe: " + severeOEs);
				if(severeOEs.size() == 0){
					LOG.info("No severe OE, do nothing");
					return new Pair<Prescription, Map<String, Double>>(new Prescription(), null);
				}

				// Put severe OEs in waiting list.
				List<String> sources = new LinkedList<>();
				for(String oe: severeOEs){
					sources.add(oe);
				}

				Map<String, String> substreamsToMigrate = new HashMap<>();
				for(String oe: sources){
					double serviceRate = examiner.model.executorServiceRate.get(oe);
					double arrivalRate = examiner.model.executorArrivalRate.get(oe);
					long backlog = examiner.model.executorBacklog.get(oe);
					TreeMap<Long, List<String>> sortedSubstream = new TreeMap<>();
					for(String sub: executorMapping.get(oe)){
						if(substreamUnlockTime.getOrDefault(sub, -100L) < examiner.state.currentTimeIndex) {
							long sbacklog = examiner.state.getSubstreamCompleted(examiner.state.substreamIdFromStringToInt(sub), examiner.state.currentTimeIndex) - examiner.state.getSubstreamCompleted(examiner.state.substreamIdFromStringToInt(sub), examiner.state.currentTimeIndex - 1);
							if (!sortedSubstream.containsKey(sbacklog)) {
								sortedSubstream.put(sbacklog, new LinkedList<>());
							}
							sortedSubstream.get(sbacklog).add(sub);
						}
					}
					while (!isExecutorSafe(backlog, serviceRate, arrivalRate, migrationTime) && sortedSubstream.size() > 0 && (sortedSubstream.size() > 1 || sortedSubstream.firstEntry().getValue().size() > 1)) {
						String sub = sortedSubstream.firstEntry().getValue().get(0);
						long subBacklog = examiner.state.getSubstreamArrived(examiner.state.substreamIdFromStringToInt(sub), examiner.state.currentTimeIndex) - examiner.state.getSubstreamCompleted(examiner.state.substreamIdFromStringToInt(sub), examiner.state.currentTimeIndex);
						double subArrival = examiner.model.substreamArrivalRate.get(sub);
						substreamsToMigrate.put(sub, oe);
						arrivalRate -= subArrival;
						backlog -= subBacklog;
						sortedSubstream.firstEntry().getValue().remove(0);
						if (sortedSubstream.firstEntry().getValue().size() == 0) {
							sortedSubstream.pollFirstEntry();
						}
					}
				}

				for(int scaleOutNumber = 0; scaleOutNumber + executorMapping.size() <= maxNumberOfExecutors; scaleOutNumber ++){
					LOG.info("Try scale out " + scaleOutNumber + " oes.");
					Set<String> tgts = new HashSet<>();
					Map<String, Map.Entry<String, String>> migratingSubstreams = new TreeMap<>();
					//Find potential targets
					Map<String, List<Object>> potentialTgts = new HashMap<>();
					double minServiceRate = examiner.model.executorServiceRate.get(sources.get(0));
					for(String oe: activatedOEs){
						if(!severeOEs.contains(oe)) {
							long backlog = examiner.model.executorBacklog.get(oe);
							double arrivalRate = examiner.model.executorArrivalRate.get(oe);
							double serviceRate = examiner.model.executorServiceRate.get(oe);
							if(isExecutorSafe(backlog, serviceRate, arrivalRate, migrationTime)) {
								List<Object> tlist = new ArrayList<>();
								tlist.add(backlog);
								tlist.add(arrivalRate);
								tlist.add(serviceRate);
								potentialTgts.put(oe, tlist);
								if (serviceRate < minServiceRate) {
									minServiceRate = serviceRate;
								}
							}
						}
					}
					LOG.info("Scale out OEs with " + minServiceRate);
					for(int i = 0; i < scaleOutNumber; i++){
						long newExecutorId = nextExecutorID.get() + i;
						String tgtExecutor = String.format("%06d", newExecutorId);
                        /*if (newExecutorId + 1 > nextExecutorID.get()) {
                            nextExecutorID.set(newExecutorId + 1);
                        }
                        numberOfOE ++;
                        */
						List<Object> tlist = new ArrayList<>();
						tlist.add(0L);
						tlist.add(0.0);
						tlist.add(minServiceRate);
						potentialTgts.put(tgtExecutor, tlist);
					}
					LOG.info("Potential Tgts: " + potentialTgts.keySet());

					boolean allSubstreamsAreMigrated = true;
					for(String sub: substreamsToMigrate.keySet()){
						if(allSubstreamsAreMigrated) {
							LOG.info("Try find target for substream " + sub);
							long subBacklog = examiner.state.getSubstreamArrived(examiner.state.substreamIdFromStringToInt(sub), examiner.state.currentTimeIndex) - examiner.state.getSubstreamCompleted(examiner.state.substreamIdFromStringToInt(sub), examiner.state.currentTimeIndex);
							double subArrival = examiner.model.substreamArrivalRate.get(sub);
							//Find a suitable OE, if multiple targets exist, choose the one with minimum backlogDelay
							String finalTgt = "";
							double minBacklogDelay = 0;
							for (String tgt : potentialTgts.keySet()) {
								long tBacklog = (Long) potentialTgts.get(tgt).get(0);
								double tArrival = (Double) potentialTgts.get(tgt).get(1);
								double tService = (Double) potentialTgts.get(tgt).get(2);
								// Debug
								//LOG.info("Tgt " + tgt + " b, a, s " + (tBacklog + subBacklog) + ", " + (tArrival + subArrival) + ", " + tService);
								if (isExecutorSafe(tBacklog + subBacklog, tService, tArrival + subArrival, migrationTime)) {
									double tBacklogDelay = (tBacklog + subBacklog + (tArrival + subArrival) * migrationTime) / tService;
									if (finalTgt.equals("") || tBacklogDelay < minBacklogDelay) {
										finalTgt = tgt;
										minBacklogDelay = tBacklogDelay;
									}
								}
							}
							if (!finalTgt.equals("")) { // Find target.
								//Debugging
								LOG.info("Migrate " + sub + " to " + finalTgt);
								long tBacklog = (Long) potentialTgts.get(finalTgt).get(0);
								double tArrival = (Double) potentialTgts.get(finalTgt).get(1);
								potentialTgts.get(finalTgt).set(0, tBacklog + subBacklog);
								potentialTgts.get(finalTgt).set(1, tArrival + subArrival);
								if (!tgts.contains(finalTgt)) tgts.add(finalTgt);
								migratingSubstreams.put(sub, new AbstractMap.SimpleEntry<>(substreamsToMigrate.get(sub), finalTgt));
							} else {
								LOG.info("Cannot find target oe for " + sub);
								allSubstreamsAreMigrated = false;
							}
						}
					}
					if(allSubstreamsAreMigrated){
						LOG.info("Find strategy with extra" + scaleOutNumber + " OEs.");
						long newExecutorId = nextExecutorID.get();
						if (newExecutorId + scaleOutNumber > nextExecutorID.get()) {
							nextExecutorID.set(newExecutorId + scaleOutNumber);
						}
						return new Pair<>(new Prescription(new ArrayList<String>(sources), new ArrayList<String>(tgts), migratingSubstreams), null);
					}else if(scaleOutNumber + executorMapping.size() == maxNumberOfExecutors){
						LOG.info("Cannot find valid strategy, try to load-balance under maximum parallelism.");
						tgts.clear();
						migratingSubstreams.clear();
						potentialTgts.clear();
						minServiceRate = examiner.model.executorServiceRate.get(sources.get(0));
						for(String oe: activatedOEs){
							if(!severeOEs.contains(oe)) {
								long backlog = examiner.model.executorBacklog.get(oe);
								double arrivalRate = examiner.model.executorArrivalRate.get(oe);
								double serviceRate = examiner.model.executorServiceRate.get(oe);
								if(isExecutorSafe(backlog, serviceRate, arrivalRate, migrationTime)) {
									List<Object> tlist = new ArrayList<>();
									tlist.add(backlog);
									tlist.add(arrivalRate);
									tlist.add(serviceRate);
									potentialTgts.put(oe, tlist);
									if (serviceRate < minServiceRate) {
										minServiceRate = serviceRate;
									}
								}
							}
						}
						if(scaleOutNumber > substreamsToMigrate.size()){
							scaleOutNumber = substreamsToMigrate.size();
						}
						HashSet<String> emptyOEs = new HashSet<>();
						LOG.info("Scale out OEs with " + minServiceRate);
						for(int i = 0; i < scaleOutNumber; i++){
							long newExecutorId = nextExecutorID.get() + i;
							String tgtExecutor = String.format("%06d", newExecutorId);
							List<Object> tlist = new ArrayList<>();
							tlist.add(0L);
							tlist.add(0.0);
							tlist.add(minServiceRate);
							potentialTgts.put(tgtExecutor, tlist);
							emptyOEs.add(tgtExecutor);
						}
						LOG.info("Potential Tgts: " + potentialTgts.keySet());
						if(potentialTgts.size() > 0) {
							for (String sub : substreamsToMigrate.keySet()) {
								long subBacklog = examiner.state.getSubstreamArrived(examiner.state.substreamIdFromStringToInt(sub), examiner.state.currentTimeIndex) - examiner.state.getSubstreamCompleted(examiner.state.substreamIdFromStringToInt(sub), examiner.state.currentTimeIndex);
								double subArrival = examiner.model.substreamArrivalRate.get(sub);
								String finalTgt = "";
								double minBacklogDelay = 0;
								for (String tgt : potentialTgts.keySet()) {
									long tBacklog = (Long) potentialTgts.get(tgt).get(0);
									double tArrival = (Double) potentialTgts.get(tgt).get(1);
									double tService = (Double) potentialTgts.get(tgt).get(2);
									double tBacklogDelay = (tBacklog + subBacklog + (tArrival + subArrival) * migrationTime) / tService;
									if (emptyOEs.contains(tgt)){
										finalTgt = tgt;
										break;
									}else if (finalTgt.equals("") || tBacklogDelay < minBacklogDelay) {
										finalTgt = tgt;
										minBacklogDelay = tBacklogDelay;
									}

								}
								LOG.info("Migrate " + sub + " to " + finalTgt);
								long tBacklog = (Long) potentialTgts.get(finalTgt).get(0);
								double tArrival = (Double) potentialTgts.get(finalTgt).get(1);
								potentialTgts.get(finalTgt).set(0, tBacklog + subBacklog);
								potentialTgts.get(finalTgt).set(1, tArrival + subArrival);
								if (!tgts.contains(finalTgt)) tgts.add(finalTgt);
								if (emptyOEs.contains(finalTgt)) emptyOEs.remove(finalTgt);
								migratingSubstreams.put(sub, new AbstractMap.SimpleEntry<>(substreamsToMigrate.get(sub), finalTgt));
							}
							long newExecutorId = nextExecutorID.get();
							if (newExecutorId + scaleOutNumber > nextExecutorID.get()) {
								nextExecutorID.set(newExecutorId + scaleOutNumber);
							}
							return new Pair<>(new Prescription(new ArrayList<String>(sources), new ArrayList<String>(tgts), migratingSubstreams), null);
						}else{
							LOG.info("Cannot find valid strategy under maximum parallelism, do nothing.");
							return new Pair<Prescription, Map<String, Double>>(new Prescription(), null);
						}
					}else{
						LOG.info("Cannot find valid strategy, try more oes.");
					}
				}
				LOG.info("Cannot find valid strategy under maximum parallelism, do nothing.");
				return new Pair<Prescription, Map<String, Double>>(new Prescription(), null);
			}

			/*
              Doing following until no OE is find:
              Find minimal OE with minimum L
              Try to distribute its substreams.
              Find target OE that b'/mu' + Tm < L and lambda' < mu'. If multiple, choose the one with minimum L'.
              If ok to distribute all, then migrate.
            */
			private Pair<Prescription, Map<String, Double>> scaleInByBacklog(Set<String> oes, long migrationTime){
				LOG.info("Try to scale in...");
				if(oes.size() <= 1){
					LOG.info("Not enough executor to scale in");
					return new Pair<Prescription, Map<String, Double>>(new Prescription(), null);
				}
				HashMap<String, List<Object>> activeOEs = new HashMap<>();
				Set<String> tgts = new HashSet<>();
				List<String> srcs = new LinkedList<>();
				Map<String, Map.Entry<String, String>> migratingSubstreams = new TreeMap<>();
				for(String oe: oes){
					long backlog = examiner.model.executorBacklog.get(oe);
					double arrival = examiner.model.executorArrivalRate.get(oe);
					double service = examiner.model.executorServiceRate.get(oe);
					List<Object> tlist = new ArrayList<>();
					tlist.add(backlog);
					tlist.add(arrival);
					tlist.add(service);
					activeOEs.put(oe, tlist);
				}
				//Be conservative, only scale in by one
				//Find minimum backlog delay oe as src
				String minSrc = null;
				double minB = 0.0;
				for(String oe: activeOEs.keySet()){
					long backlog = examiner.model.executorBacklog.get(oe);
					double serviceRate = examiner.model.executorServiceRate.get(oe);
					double arrivalRate = examiner.model.executorArrivalRate.get(oe);
					if(minSrc == null || (backlog + arrivalRate * migrationTime) / serviceRate < minB){
						minSrc = oe;
					}
				}

				if(minSrc == null){
					LOG.info("No oe to scaled in");
					//break;
					return new Pair<Prescription, Map<String, Double>>(new Prescription(), null);
				}
				LOG.info("Try to scale in " + minSrc);
				//Try to distribute its substreams
				Map<String, String> dest = new HashMap<>();
				for(String sub: executorMapping.get(minSrc)) {
					if (substreamUnlockTime.getOrDefault(sub, -100000l) >= examiner.state.currentTimeIndex) {
						LOG.info("Source has locked substream, cannot scale-in.");
						return new Pair<Prescription, Map<String, Double>>(new Prescription(), null);
					}
				}

				for(String sub: executorMapping.get(minSrc)) {
					long subBacklog = examiner.state.getSubstreamArrived(examiner.state.substreamIdFromStringToInt(sub), examiner.state.currentTimeIndex) - examiner.state.getSubstreamCompleted(examiner.state.substreamIdFromStringToInt(sub), examiner.state.currentTimeIndex);
					double subArrival = examiner.model.substreamArrivalRate.get(sub);
					String tgtOE = "";
					// If there is multiple targets, choose the OE with minimum backlog latency.
					double minTargetBacklogLatency = 0;
					for(String oe: activeOEs.keySet()){
						if(!oe.equals(minSrc)){
							long tBacklog = (Long)activeOEs.get(oe).get(0);
							double tArrival = (Double)activeOEs.get(oe).get(1);
							double tService = (Double) activeOEs.get(oe).get(2);
							if(isExecutorSafe(tBacklog + subBacklog, tService, tArrival + subArrival, migrationTime)) {
								double tBacklogLatency = (tBacklog + subBacklog + (tArrival + subArrival) * migrationTime) / tService;
								if(tgtOE.equals("") || tBacklogLatency < minTargetBacklogLatency){
									tgtOE = oe;
									minTargetBacklogLatency = tBacklogLatency;
								}
							}
						}
					}
					if(tgtOE.equals("")) {
						LOG.info("Cannot find target OE for substream " + sub);
						return new Pair<Prescription, Map<String, Double>>(new Prescription(), null);
					}else{
						dest.put(sub, tgtOE);
						activeOEs.get(tgtOE).set(0, (Long)activeOEs.get(tgtOE).get(0) + subBacklog);
						activeOEs.get(tgtOE).set(1, (Double)activeOEs.get(tgtOE).get(1) + subArrival);
					}
				}

				LOG.info("OK to scale in " + dest);
				srcs.add(minSrc);
				for(String sub: dest.keySet()){
					tgts.add(dest.get(sub));
					migratingSubstreams.put(sub, new AbstractMap.SimpleEntry<>(minSrc, dest.get(sub)));
				}
				dest.clear();
				activeOEs.remove(minSrc);
				return new Pair<>(new Prescription(srcs, new ArrayList<String>(tgts), migratingSubstreams), null);
			}


			//Iterate all pairs of source and targe OE, find the one minimize delay vector
			/* private Pair<Prescription, Map<String, Double>> scaleIn(Set<String> oes){
				LOG.info("Try to scale in");
				if(oes.size() <= 1){
					LOG.info("Not enough executor to merge");
					return new Pair<Prescription, Map<String, Double>>(new Prescription(), null);
				}
				//Only consider unlocked oes
				//HashSet<String> unlockedOEs = new HashSet<String>(executorMapping.keySet());
				//unlockedOEs.removeAll(oeUnlockTime.keySet());

				String minsrc = "", mintgt = "";
				List<Double> best = null;

				for(String src: oes){
					double srcArrival = examiner.model.executorArrivalRate.get(src);
					for(String tgt: oes)
						if(!tgt.equals(src)){
							double tgtArrival = examiner.model.executorArrivalRate.get(tgt);
							double tgtService = examiner.model.executorServiceRate.get(tgt);
							//Try to migrate all substreams from src to tgt
							//if(srcArrival + tgtArrival < tgtService){
							double estimatedLongtermDelay = examiner.model.calculateLongTermDelay(srcArrival + tgtArrival, tgtService);
							List<Double> current = new ArrayList<>();
							for(String executor: oes){
								if(executor.equals(src)){
									current.add(0.0);
								}else if(executor.equals(tgt)){
									current.add(estimatedLongtermDelay);
								}else{
									current.add(examiner.model.getLongTermDelay(executor));
								}
							}
							current.sort(Collections.reverseOrder());
							if(best == null || vectorGreaterThan(best, current)){
								best = current;
								minsrc = src;
								mintgt = tgt;
							}
							//}
						}
				}


				if(best != null){
					List<String> migratingSubstreams = new ArrayList<>(executorMapping.get(minsrc));
					LOG.info("Scale in! from " + minsrc + " to " + mintgt);
					LOG.info("Migrating partitions: " + migratingSubstreams);
					HashMap<String, Double> map = new HashMap<>();
					for(String executor: oes){
						if(executor.equals(minsrc)){
							map.put(minsrc, 0.0);
						}else if(executor.equals(mintgt)){
							double arrival = examiner.model.executorArrivalRate.get(mintgt) + examiner.model.executorArrivalRate.get(mintgt);
							double service = examiner.model.executorServiceRate.get(mintgt);
							map.put(mintgt, examiner.model.calculateLongTermDelay(arrival, service));
						}else{
							map.put(executor, examiner.model.getLongTermDelay(executor));
						}
					}
					return new Pair<Prescription, Map<String, Double>>(new Prescription(minsrc, mintgt, migratingSubstreams), map);
				}else {
					LOG.info("Cannot find any scale in");
					return new Pair<Prescription, Map<String, Double>>(new Prescription(), null);
				}
			} */

			/* private Pair<Prescription, Map<String, Double>> balanceLoad(Set<String> oes){
				LOG.info("Try to migrate");
				//LOG.info("Migrating once based on assignment: " + executorMapping);
				if (oes.size() == 0) { //No executor to move
					return new Pair<Prescription, Map<String, Double>>(new Prescription(), null);
				}

				//Find container with maximum delay
				Pair<String, Double> a = getWorstExecutor(oes);
				String srcExecutor = a.getKey();
				if (srcExecutor.equals("")) { //No correct container
					LOG.info("Cannot find the container that exceeds threshold");
					return new Pair<Prescription, Map<String, Double>>(new Prescription(), null);
				}

				if (executorMapping.get(srcExecutor).size() <= 1) { //Container has only one substream
					LOG.info("Largest delay container " + srcExecutor + " has only " + executorMapping.get(srcExecutor).size());
					return new Pair<Prescription, Map<String, Double>>(new Prescription(), null);
				}
				LOG.info("Try to migrate from largest delay container " + srcExecutor);
				String bestTgtExecutor = null;
				List<Double> best = null;
				List<String> bestMigratingSubstreams = null;
				for (String tgtExecutor : oes)
					if (!srcExecutor.equals(tgtExecutor)) {
						double tgtArrivalRate = examiner.model.executorArrivalRate.get(tgtExecutor);
						double tgtServiceRate = examiner.model.executorServiceRate.get(tgtExecutor);
						if (tgtArrivalRate < tgtServiceRate - 1e-9) {
							TreeMap<Double, Set<String>> substreams = new TreeMap<>();
							for(String substream: executorMapping.get(srcExecutor)){
								double delay = examiner.state.getSubstreamLatency(examiner.state.substreamIdFromStringToInt(substream), examiner.state.currentTimeIndex) /
									((double) examiner.state.getSubstreamCompleted(examiner.state.substreamIdFromStringToInt(substream), examiner.state.currentTimeIndex) - examiner.state.getSubstreamCompleted(examiner.state.substreamIdFromStringToInt(substream), examiner.state.currentTimeIndex - 1));
								substreams.putIfAbsent(delay, new HashSet<>());
								substreams.get(delay).add(substream);
							}

							//Debugging
							LOG.info("Debugging, substreams' latency=" + substreams);
							List<String> sortedSubstreams = new LinkedList<>();
							for(Map.Entry<Double, Set<String>> entry: substreams.entrySet()){
								sortedSubstreams.addAll(entry.getValue());
							}

							double srcArrivalRate = examiner.model.executorArrivalRate.get(srcExecutor);
							double srcServiceRate = examiner.model.executorServiceRate.get(srcExecutor);
							List<String> migrating = new ArrayList<>();
							//LOG.info("Debugging, try to migrate to " + tgtExecutor + "tgt la=" + tgtArrivalRate + "tgt mu=" + tgtServiceRate);
							for(String substream: sortedSubstreams){ //Cannot migrate all substreams out?
								double arrival = examiner.model.substreamArrivalRate.get(substream);
								srcArrivalRate -= arrival;
								tgtArrivalRate += arrival;
								migrating.add(substream);
								//if(srcArrivalRate < srcServiceRate && tgtArrivalRate < tgtServiceRate){
								double srcDelay = examiner.model.calculateLongTermDelay(srcArrivalRate, srcServiceRate),
									tgtDelay = examiner.model.calculateLongTermDelay(tgtArrivalRate, tgtServiceRate);
								//LOG.info("Debugging, current src la=" + srcArrivalRate + " tgt la=" + tgtArrivalRate + " substreams=" + migrating);
								List<Double> current = new ArrayList<>();
								for(String executor: oes){
									if(executor.equals(srcExecutor)){
										current.add(srcDelay);
									}else if(executor.equals(tgtExecutor)){
										current.add(tgtDelay);
									}else {
										current.add(examiner.model.getLongTermDelay(executor));
									}
								}
								current.sort(Collections.reverseOrder()); //Delay vector is from largest to smallest.

								//LOG.info("Debugging, vectors=" + current);
								if(best == null || vectorGreaterThan(best, current)){
									best = current;
									bestTgtExecutor = tgtExecutor;
									if(bestMigratingSubstreams != null)bestMigratingSubstreams.clear();
									bestMigratingSubstreams = new ArrayList<>(migrating);
								}
								if(tgtDelay > srcDelay)break;
								//}
								if(tgtArrivalRate > tgtServiceRate)break;
							}
						}
					}
				if(best == null){
					LOG.info("Cannot find any migration");
					return new Pair<>(new Prescription(), null);
				}
				LOG.info("Find best migration with delay: " + best + ", from executor " + srcExecutor + " to executor " + bestTgtExecutor + " migrating Partitions: " + bestMigratingSubstreams);
				Map<String, Double> map = new HashMap<>();
				double srcArrival = examiner.model.executorArrivalRate.get(srcExecutor);
				double tgtArrival = examiner.model.executorArrivalRate.get(bestTgtExecutor);
				double srcService = examiner.model.executorServiceRate.get(srcExecutor);
				double tgtService = examiner.model.executorServiceRate.get(bestTgtExecutor);
				for(String substream: bestMigratingSubstreams){
					double arrival = examiner.model.substreamArrivalRate.get(substream);
					srcArrival -= arrival;
					tgtArrival += arrival;
				}
				map.put(srcExecutor, examiner.model.calculateLongTermDelay(srcArrival, srcService));
				map.put(bestTgtExecutor, examiner.model.calculateLongTermDelay(tgtArrival, tgtService));
				for(String executor: oes){
					if(!executor.equals(srcExecutor) && !executor.equals(bestTgtExecutor)){
						map.put(executor, examiner.model.getLongTermDelay(executor));
					}
				}
				return new Pair<>(new Prescription(srcExecutor, bestTgtExecutor, bestMigratingSubstreams), map);
			} */

			private final static int GOOD = 0, MODERATE = 1, SEVERE = 2;
		}

		Diagnoser diagnoser = new Diagnoser();

		//Only consider unlocked oes
		HashSet<String> unlockedOEs = new HashSet<String>(executorMapping.keySet());
		unlockedOEs.removeAll(oeUnlockTime.keySet());
		unlockedOEs.removeAll(examiner.model.invalidExecutors);

		Prescription pres = new Prescription(null, null, null);
		LOG.info("Debugging, instant delay vector: " + examiner.getInstantDelay() + " long term delay vector: " + examiner.getLongtermDelay());
        /*if(isMigrating){
            LOG.info("Migration does not complete, cannot diagnose");
            return pres;
        }*/
		if(diagnoser.checkSafe(unlockedOEs, examiner.model.maximumMigrationTime)){
			// Try to scale in if no invalid OE
			LOG.info("All OEs are safe, try to scale-in...");
			if(examiner.model.invalidExecutors.isEmpty()) {
				//Try scale in
				Pair<Prescription, Map<String, Double>> result = diagnoser.scaleInByBacklog(unlockedOEs, examiner.model.maximumMigrationTime);
				return result.getKey();

			}else{
				LOG.info("Current healthiness is Good. But some substreams are invalid, do nothing");
				return pres;
			}
		}else{
			// Deal with severe and caution OEs.
			LOG.info("Some OEs are not safe, finding severe OEs...");
			Set<String> severeOEs = diagnoser.findSevereOEs(unlockedOEs, examiner.model.maximumMigrationTime);
			System.out.println("Number of severe OEs: " + severeOEs.size());

			if(severeOEs.isEmpty()){
				LOG.info("No severe OEs, do nothing.");
				return pres;
			}else{
				LOG.info("Load-balance and scale-out severe OEs...");
				Pair<Prescription, Map<String, Double>> result = diagnoser.loadBalanceAndScaleOut(unlockedOEs, severeOEs, examiner.model.maximumMigrationTime);
				return result.getKey();
			}
		}
	}

	//Return state validity
	private boolean examine(long timeIndex){
		Map<String, Object> metrics = metricsRetriever.retrieveMetrics();
		Map<String, Long> substreamArrived =
			(HashMap<String, Long>) (metrics.get("Arrived"));
		Map<String, Long> substreamProcessed =
			(HashMap<String, Long>) (metrics.get("Processed"));
		Map<String, Boolean> substreamValid =
			(HashMap<String,Boolean>)metrics.getOrDefault("Validity", null);
		Map<String, Double> executorServiceRate =
			(HashMap<String, Double>) (metrics.get("ServiceRate"));
		//Memory usage
		//LOG.info("Metrics size arrived size=" + substreamArrived.size() + " processed size=" + substreamProcessed.size() + " valid size=" + substreamValid.size() + " utilization size=" + executorUtilization.size());
		if(examiner.updateState(timeIndex, substreamArrived, substreamProcessed, substreamValid, executorMapping)){
			examiner.updateModel(timeIndex, executorServiceRate, substreamValid, executorMapping);
			return true;
		}
		return false;
	}

	//Treatment for Samza
	private void treat(Prescription pres){
		if (!isTreat) {
			return;
		} else {
			if (pres.migratingSubstreams == null) {
				LOG.warn("Prescription has nothing, so do no treatment");
				return;
			}

			pendingPres = pres;
			isMigrating = true;

			LOG.info("Old mapping: " + executorMapping);
			Map<String, List<String>> newAssignment = pres.generateNewSubstreamAssignment(executorMapping);
			LOG.info("Prescription : src: " + pres.sources + " , tgt: " + pres.targets + " , migrating: " + pres.migratingSubstreams);
			LOG.info("New mapping: " + newAssignment);
			System.out.println("jobid: " + jobid + " New mapping at time: " + examiner.state.currentTimeIndex + " mapping: " + newAssignment);

			if(examiner.model.selfAdaptiveMigrationTimeFlag){
				for(String substream: pres.migratingSubstreams.keySet()){
					examiner.model.substreamLastDecisionTime.put(substream, -examiner.state.currentTimeIndex);
				}
			}

			//Scale out
			boolean isScaleOut = false;
			for(String tgt: pres.targets){
				if(!executorMapping.containsKey(tgt)) {
					isScaleOut = true;
				}
			}
			if (isScaleOut) {
				LOG.info("Scale out");
				//For drawing figure
				System.out.println("jobid: " + jobid + " Migration! Scale out prescription at time: " + examiner.state.currentTimeIndex +  " from executor " + pres.sources + " to executor " + pres.targets);

				listener.scale(newAssignment.size(), newAssignment);
			} else {
				int total = 0;
				for(String src: pres.sources)
					total += executorMapping.get(src).size();
				//Scale in
				if (total == pres.migratingSubstreams.size()) {
					LOG.info("Scale in");
					//For drawing figure
					System.out.println("jobid: " + jobid + " Migration! Scale in prescription at time: " + examiner.state.currentTimeIndex + " from executor " + pres.sources + " to executor " + pres.targets);

					listener.scale(newAssignment.size(), newAssignment);
				}
				//Load balance
				else {
					LOG.info("Load balance");
					//For drawing figure
					System.out.println("jobid: " + jobid + " Migration! Load balance prescription at time: " + examiner.state.currentTimeIndex + " from executor " + pres.sources + " to executor " + pres.targets);

					listener.remap(newAssignment);
				}
			}
		}
	}

	//Main logic:  examine->diagnose->treatment->sleep
	void work(long timeIndex) {
		LOG.info("Examine...");
		//Examine
		boolean stateValidity = examine(timeIndex);

		//Update oe migration locks
		for(String oe: new ArrayList<String>(oeUnlockTime.keySet())){
			if(oeUnlockTime.get(oe) < timeIndex){
				oeUnlockTime.remove(oe);
			}
		}
		LOG.info("Locked OEs: " + oeUnlockTime);

		// Check valid OEs:
		LOG.info("Invalid OEs: " + examiner.model.invalidExecutors);


		//Check is started
		if(!isStarted){
			LOG.info("Check started...");
			for(int id: examiner.state.substreamStates.keySet()){
				if(examiner.state.substreamStates.get(id).arrived.containsKey(timeIndex) && examiner.state.substreamStates.get(id).arrived.get(timeIndex) != null && examiner.state.substreamStates.get(id).arrived.get(timeIndex) > 0){
					isStarted = true;
					break;
				}
			}
		}

		if (stateValidity && !isMigrating && isStarted){
			LOG.info("Diagnose...");
			//Diagnose
			Prescription pres = diagnose(examiner);
			if (pres.migratingSubstreams != null) {
				//Treatment
				treat(pres);
			} else {
				LOG.info("Nothing to do this time.");
			}
		} else {
			if (!stateValidity) LOG.info("Current examine data is not valid, need to wait until valid");
			else if (isMigrating) LOG.info("One migration is in process");
			else LOG.info("Too close to last migration");
		}
	}

	@Override
	public synchronized void onMigrationExecutorsStopped(){
		LOG.info("Migration executors stopped, try to acquire lock...");
		updateLock.lock();
		try {
			if (examiner == null) {
				LOG.warn("Examiner haven't been initialized");
			} if(pendingPres == null){// else if (!isMigrating || pendingPres == null) {
				LOG.warn("There is no pending migration, please checkout");
			} else {
				String migrationType = "migration";
				// For self-adaptive maximum migration time
				if(examiner.model.selfAdaptiveMigrationTimeFlag){
					for(String substream: pendingPres.migratingSubstreams.keySet()){
						if(!examiner.model.substreamLastDecisionTime.containsKey(substream)) {
							LOG.warn("Cannot find last decision time for substream " + substream + ", please checkout!");
						}
						long tTimeIndex = examiner.model.substreamLastDecisionTime.getOrDefault(substream, examiner.state.currentTimeIndex);
						examiner.model.substreamLastDecisionTime.put(substream, -tTimeIndex);
					}
				}
				//Scale in, remove useless information
				//Scale in, remove useless information
				int totalSubstreams = 0;
				for(String oe: pendingPres.sources){
					totalSubstreams += executorMapping.get(oe).size();
				}
				if(pendingPres.migratingSubstreams.size() == totalSubstreams){
					for(String src: pendingPres.sources) {
						examiner.model.executorServiceRate.remove(src);
						examiner.model.executorInstantaneousDelay.remove(src);
						examiner.model.executorBacklogDelay.remove(src);
						examiner.model.executorBacklog.remove(src);
						examiner.model.executorArrived.remove(src);
					}
					migrationType = "scale-in";
				}
				for(String tgt: pendingPres.targets) {
					if (!executorMapping.containsKey(tgt)) {
						migrationType = "scale-out";
						break;
					}
				}
				//For drawing figre
				LOG.info("Migrating " + pendingPres.migratingSubstreams + " from " + pendingPres.sources + " to " + pendingPres.targets);

				long unlockTime = examiner.state.currentTimeIndex + (migrationInterval / metricsRetreiveInterval);
				for(String source: pendingPres.sources) {
					oeUnlockTime.put(source, unlockTime);
				}
				for(String target: pendingPres.targets) {
					oeUnlockTime.put(target, unlockTime);
				}
				long substreamsUnlockTime = examiner.state.currentTimeIndex + (config.getLong("streamswitch.system.maxmigrationtime", 500) / metricsRetreiveInterval);
				for(String substream: pendingPres.migratingSubstreams.keySet()){
					substreamUnlockTime.put(substream, substreamsUnlockTime);
				}
				isMigrating = false;
				System.out.println("jobid: " + jobid + " Executors stopped at time " + examiner.state.currentTimeIndex + " : " + migrationType + " from " + pendingPres.sources + " to " + pendingPres.targets);

				executorMapping = pendingPres.generateNewSubstreamAssignment(executorMapping);
				pendingPres = null;
			}
		}finally {
			updateLock.unlock();
			LOG.info("Mapping changed, unlock");
		}
	}
	@Override
	public void onMigrationCompleted(){
        /*
        LOG.info("Migration completed, try to acquire lock...");
        updateLock.lock();
        try {
            LOG.info("Lock acquired, set migrating flag to false");
            System.out.println("Migration completed at time " + (System.currentTimeMillis() - startTime)/metricsRetreiveInterval);
            isMigrating = false;
        }finally {
            updateLock.unlock();
            LOG.info("Migration completed, unlock");
        }
        */
	}
}
