package de.hpi.dbda;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/*
 * Maps every Tuple2<candidate id, count> to Tuple2<actual candidate as IntArray, id>
 */
public class DeltaCalculator
		extends
		RichMapFunction<Tuple2<Integer, Integer>, Tuple2<IntArray, Integer>> {
	private static final long serialVersionUID = 3213741546556709358L;

	public static final String FIRST_ROUND_NAME = "firstRound";

	private List<IntArray> candidateLookup;
	private boolean firstRound;

	/*
	 * Maps one Tuple2<candidate id, count> to Tuple2<actual candidate as IntArray, id>
	 * If firstRound == true then a new IntArray is created.
	 */
	public Tuple2<IntArray, Integer> spellOutLargeItem(
			Tuple2<Integer, Integer> largeItemSet) {
		if (firstRound) {
			return new Tuple2<IntArray, Integer>(new IntArray(
					new int[] { largeItemSet.f0 }), largeItemSet.f1);
		} else {
			return new Tuple2<IntArray, Integer>(
					candidateLookup.get(largeItemSet.f0), largeItemSet.f1);
		}
	}

	/*
	 * Gets candidateLookup and firstRound from the corresponding Flink broadcasts.
	 */
	@Override
	public void open(org.apache.flink.configuration.Configuration parameters)
			throws Exception {
		firstRound = (Boolean) getRuntimeContext().getBroadcastVariable(
				DeltaCalculator.FIRST_ROUND_NAME).get(0);
		if (firstRound) {
			candidateLookup = new ArrayList<IntArray>();
		} else {
			candidateLookup = (List<IntArray>) getRuntimeContext().getBroadcastVariable(
					TrieBuilder.CANDIDATE_LOOKUP_NAME).get(0);
		}
	};

	/*
	 * Simply calls spellOutLargeItem and returns the result.
	 */
	@Override
	public Tuple2<IntArray, Integer> map(Tuple2<Integer, Integer> largeItem)
			throws Exception {
		Tuple2<IntArray, Integer> result = spellOutLargeItem(largeItem);
		return result;
	}

}
