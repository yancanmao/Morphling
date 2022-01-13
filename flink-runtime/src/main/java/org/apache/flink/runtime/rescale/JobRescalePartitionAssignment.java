package org.apache.flink.runtime.rescale;

import org.apache.flink.runtime.state.KeyGroupRange;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public class JobRescalePartitionAssignment {

	public static final int UNUSED_SUBTASK = Integer.MAX_VALUE/2;

	private final int numOpenedSubtask;

	private final JobRescalePartitionAssignment oldRescalePA;

	// subtaskIndex -> partitions
	private final Map<Integer, List<Integer>> partitionAssignment;

	// subtaskIndex (in flink) -> idInModel (in streamswitch)
	private final Map<Integer, Integer> subtaskIndexMapping;

	// subtaskIndex (in flink) -> idInModel (in streamswitch, or said executorId)
	private final Map<Integer, Integer> executorIdMapping;

	private final List<KeyGroupRange> alignedKeyGroupRanges;

	private final Map<Integer, Boolean> modifiedSubtaskMap;

	// this is used for remove the correponding subtask
	private final Map<Integer, Boolean> removedSubtaskMap;

	public JobRescalePartitionAssignment(
		Map<String, List<String>> strExecutorMapping,
		Map<String, List<String>> strOldExecutorMapping,
		JobRescalePartitionAssignment oldRescalePA,
		int numOpenedSubtask) {

		this.numOpenedSubtask = numOpenedSubtask;
		this.oldRescalePA = checkNotNull(oldRescalePA);

		checkState(checkPartitionAssignmentValidity(strExecutorMapping),
			"executorMapping has null or empty partition");

		checkState(checkPartitionAssignmentValidity(strOldExecutorMapping),
			"oldExecutorMapping has null or empty partition");

		this.partitionAssignment = new HashMap<>();
		this.subtaskIndexMapping = new HashMap<>();
		this.executorIdMapping = new HashMap<>();
		this.alignedKeyGroupRanges = new ArrayList<>();
		this.modifiedSubtaskMap = new HashMap<>();
		this.removedSubtaskMap = new HashMap<>();

		// here we copy and translate passed-in mapping
		Map<Integer, List<Integer>> executorMapping = generateIntegerMap(strExecutorMapping);
		Map<Integer, List<Integer>> oldExecutorMapping = generateIntegerMap(strOldExecutorMapping);

		int newParallelism = executorMapping.keySet().size();
		int oldParallelism = oldExecutorMapping.keySet().size();

		if (newParallelism > oldParallelism) {
			setupFollowScaleOut(executorMapping, oldExecutorMapping);
		} else if (newParallelism < oldParallelism) {
			setupFollowScaleIn(executorMapping, oldExecutorMapping);
		} else {
			setupFollowRepartition(executorMapping, oldExecutorMapping);
		}
		fillingUnused(executorMapping.keySet().size());

		generateAlignedKeyGroupRanges();
		generateExecutorIdMapping();
	}

	public JobRescalePartitionAssignment(
		Map<String, List<String>> strExecutorMapping,
		int numOpenedSubtask) {

		this.numOpenedSubtask = numOpenedSubtask;
		this.oldRescalePA = null;

		checkState(checkPartitionAssignmentValidity(strExecutorMapping),
			"executorMapping has null or empty partition");

		this.partitionAssignment = generateIntegerMap(strExecutorMapping);
		this.subtaskIndexMapping = initSubtaskIndexMap(numOpenedSubtask);

		this.executorIdMapping = new HashMap<>();
		this.alignedKeyGroupRanges = new ArrayList<>();
		this.modifiedSubtaskMap = new HashMap<>();
		this.removedSubtaskMap = new HashMap<>();

		generateAlignedKeyGroupRanges();
		generateExecutorIdMapping();
	}

	private void setupFollowScaleOut(
		Map<Integer, List<Integer>> executorMapping,
		Map<Integer, List<Integer>> oldExecutorMapping) {

		List<Integer> createdExecutorIdList = executorMapping.keySet().stream()
			.filter(id -> !oldExecutorMapping.containsKey(id))
			.collect(Collectors.toList());
//		checkState(createdExecutorIdList.size() == 1, "more than one created");


		List<Integer> modifiedExecutorIdList = oldExecutorMapping.keySet().stream()
			.filter(id ->
				// listEqualsIgnoreOrder(executorMapping.get(id), oldExecutorMapping.get(id))
				!(executorMapping.get(id).size() == oldExecutorMapping.get(id).size()
					&& executorMapping.get(id).containsAll(oldExecutorMapping.get(id))))
			.collect(Collectors.toList());
//		checkState(modifiedExecutorIdList.size() == 1, "more than one modified in scale out");

//		int modifiedExecutorId = modifiedExecutorIdList.get(0);

		Map<Integer, Integer> unUsedSubtaskMap = findNextUnusedSubtask(createdExecutorIdList);

		for (Map.Entry<Integer, List<Integer>> entry : executorMapping.entrySet()) {
			int executorId = entry.getKey();
			List<Integer> partition = entry.getValue();

			// if the subtask is to be created, find out the corresponding subtask index from unUsedSubtaskMap
			int subtaskIndex = (createdExecutorIdList.contains(executorId)) ?
				unUsedSubtaskMap.get(executorId):
				oldRescalePA.getSubTaskId(executorId);

			putExecutorToSubtask(subtaskIndex, executorId, partition);

			if (createdExecutorIdList.contains(executorId) || modifiedExecutorIdList.contains(executorId)) {
				modifiedSubtaskMap.put(subtaskIndex, true);
			}
		}
	}

	private void setupFollowScaleIn(
		Map<Integer, List<Integer>> executorMapping,
		Map<Integer, List<Integer>> oldExecutorMapping) {

		List<Integer> removedExecutorId = oldExecutorMapping.keySet().stream()
			.filter(id -> !executorMapping.containsKey(id))
			.collect(Collectors.toList());
		checkState(removedExecutorId.size() == 1, "more than one removed");

		int removedId = removedExecutorId.get(0);
		modifiedSubtaskMap.put(oldRescalePA.getSubTaskId(removedId), true);
		removedSubtaskMap.put(oldRescalePA.getSubTaskId(removedId), true);

		List<Integer> modifiedIdList = executorMapping.keySet().stream()
			.filter(id -> executorMapping.get(id).size() != oldExecutorMapping.get(id).size())
			.collect(Collectors.toList());
		checkState(modifiedIdList.size() == 1, "more than one modified in scale in");

		int modifiedExecutorId = modifiedIdList.get(0);

		for (Map.Entry<Integer, List<Integer>> entry : executorMapping.entrySet()) {
			int executorId = entry.getKey();
			List<Integer> partition = entry.getValue();

			int subtaskIndex = oldRescalePA.getSubTaskId(executorId);
			putExecutorToSubtask(subtaskIndex, executorId, partition);

			if (executorId == modifiedExecutorId) {
				modifiedSubtaskMap.put(subtaskIndex, true);
			}
		}
	}

	private void setupFollowRepartition(
		Map<Integer, List<Integer>> executorMapping,
		Map<Integer, List<Integer>> oldExecutorMapping) {

		List<Integer> modifiedIdList = executorMapping.keySet().stream()
			.filter(id ->
				// listEqualsIgnoreOrder(executorMapping.get(id), oldExecutorMapping.get(id))
				!(executorMapping.get(id).size() == oldExecutorMapping.get(id).size()
					&& executorMapping.get(id).containsAll(oldExecutorMapping.get(id))))
			.collect(Collectors.toList());
//		checkState(modifiedIdList.size() == 2, "not exactly two are modified in repartition");

		for (Map.Entry<Integer, List<Integer>> entry : executorMapping.entrySet()) {
			int executorId = entry.getKey();
			List<Integer> partition = entry.getValue();

			int subtaskIndex = oldRescalePA.getSubTaskId(executorId);
			putExecutorToSubtask(subtaskIndex, executorId, partition);

			if (modifiedIdList.contains(executorId)) {
				modifiedSubtaskMap.put(subtaskIndex, true);
			}
		}
	}

	private Map<Integer, Integer> findNextUnusedSubtask(List<Integer> createdExecutorIdList) {
		checkState(createdExecutorIdList.size() > 0, "null created task list");

		int n = createdExecutorIdList.size();
		Map<Integer, Integer> unUsedSubtaskMap = new HashMap<>(n);
		for (int i = 0; i < numOpenedSubtask; i++) {
			if (oldRescalePA.getIdInModel(i) == UNUSED_SUBTASK) {
				unUsedSubtaskMap.put(createdExecutorIdList.get(createdExecutorIdList.size()-n), i);
				n--;
				if (n == 0) {
					break;
				}
			}
		}
		checkState(unUsedSubtaskMap.size() > 0, "cannot find valid subtask for created executor");

		return unUsedSubtaskMap;
	}

	private int findNextUnusedSubtask() {
		int subtaskIndex = -1;
		for (int i = 0; i < numOpenedSubtask; i++) {
			if (oldRescalePA.getIdInModel(i) == UNUSED_SUBTASK) {
				subtaskIndex = i;
				break;
			}
		}
		checkState(subtaskIndex >= 0, "cannot find valid subtask for created executor");

		return subtaskIndex;
	}

	private void putExecutorToSubtask(int subtaskIndex, int executorId, List<Integer> partition) {
		Integer absent = subtaskIndexMapping.putIfAbsent(subtaskIndex, executorId);
		checkState(absent == null, "should be one-to-one mapping");

		List<Integer> absent1 = partitionAssignment.putIfAbsent(subtaskIndex, partition);
		checkState(absent1 == null, "should be one-to-one mapping");
	}

	private void fillingUnused(int newParallelism) {
		int numOccupiedSubtask = 0;
		for (int subtaskIndex = 0; subtaskIndex < numOpenedSubtask; subtaskIndex++) {
			Integer absent = subtaskIndexMapping.putIfAbsent(subtaskIndex, UNUSED_SUBTASK);
			partitionAssignment.putIfAbsent(subtaskIndex, new ArrayList<>());

			if (absent != null) {
				numOccupiedSubtask++;
			}
		}

		checkState(numOccupiedSubtask == newParallelism);
	}

	private void generateAlignedKeyGroupRanges() {
		int keyGroupStart = 0;
		for (int subTaskIndex = 0; subTaskIndex < partitionAssignment.keySet().size(); subTaskIndex++) {
			int rangeSize = partitionAssignment.get(subTaskIndex).size();

			KeyGroupRange keyGroupRange = rangeSize == 0 ?
				KeyGroupRange.EMPTY_KEY_GROUP_RANGE :
				new KeyGroupRange(
					keyGroupStart,
					keyGroupStart + rangeSize - 1,
					partitionAssignment.get(subTaskIndex));

			alignedKeyGroupRanges.add(keyGroupRange);
			keyGroupStart += rangeSize;
		}
	}

	private void generateExecutorIdMapping() {
		for (Map.Entry<Integer, Integer> entry : subtaskIndexMapping.entrySet()) {
			if (entry.getValue() != UNUSED_SUBTASK) {
				executorIdMapping.put(entry.getValue(), entry.getKey());
			}
		}
	}

	public int getNumOpenedSubtask() {
		return numOpenedSubtask;
	}

	public int getIdInModel(int subtaskIndex) {
		return subtaskIndexMapping.getOrDefault(subtaskIndex, UNUSED_SUBTASK);
	}

	public int getSubTaskId(int idInModel) {
		return executorIdMapping.get(idInModel);
	}

	public Map<Integer, List<Integer>> getPartitionAssignment() {
		return partitionAssignment;
	}

	public List<KeyGroupRange> getAlignedKeyGroupRanges() {
		return alignedKeyGroupRanges;
	}

	public KeyGroupRange getAlignedKeyGroupRange(int subTaskIndex) {
		return alignedKeyGroupRanges.get(subTaskIndex);
	}

	public boolean isSubtaskModified(int subtaskIndex) {
		return modifiedSubtaskMap.getOrDefault(subtaskIndex, false);
	}

	public List<Integer> getRemovedSubtask() {
		List<Integer> removedSubtask = new ArrayList<>();
		for (Integer removedSubtaskId : removedSubtaskMap.keySet()) {
			removedSubtask.add(removedSubtaskId);
		}
		return removedSubtask;
	}

	private static boolean checkPartitionAssignmentValidity(
		Map<String, List<String>> partitionAssignment) {

		for (List<String> partitions : partitionAssignment.values()) {
			if (partitions == null || partitions.size() == 0) {
				return false;
			}
		}
		return true;
	}

	// map of string -> map of integer
	private static Map<Integer, List<Integer>> generateIntegerMap(
		Map<String, List<String>> partitionAssignment) {

		Map<Integer, List<Integer>> mapping = new HashMap<>();
		for (String subTaskIndexStr : partitionAssignment.keySet()) {
			int subTaskIndex = Integer.parseInt(subTaskIndexStr);
			List<Integer> partitions = new ArrayList<>();

			for (String partitionStr : partitionAssignment.get(subTaskIndexStr)) {
				partitions.add(Integer.parseInt(partitionStr));
			}
			mapping.put(subTaskIndex, partitions);
		}

		return mapping;
	}

	private static Map<Integer, Integer> initSubtaskIndexMap(
		int numExecutors) {

		Map<Integer, Integer> mapping = new HashMap<>();
		for (int i = 0; i < numExecutors; i++) {
			mapping.put(i, i);
		}
		return mapping;
	}

	@Override
	public String toString() {
		return String.format("\n%s: %s\n%s: %s\n%s: %s\n%s: %s",
			"partitionAssignment", partitionAssignment,
			"subtaskIndexMapping", subtaskIndexMapping,
			"alignedKeyGroupRanges", alignedKeyGroupRanges,
			"modifiedSubtaskMap", modifiedSubtaskMap);
	}
}
