package de.hpi.dbda;

import java.util.Set;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class CandidatesListMatcher implements FlatMapFunction<IntArray, Tuple2<IntArray, Integer>> {
	private static final long serialVersionUID = 901550095279687408L;
	Set<IntArray> candidates;

	public CandidatesListMatcher(Set<IntArray> candidates) {
		this.candidates = candidates;
	}

	public void flatMap(IntArray transactionWrapper, Collector<Tuple2<IntArray, Integer>> out) throws Exception {
		int[] myCandidate;
		int[] transaction = transactionWrapper.value;
		for (IntArray myCandidateWrapper : candidates) {
			myCandidate = myCandidateWrapper.value;
			int j = 0;
			for (int i = 0; i < myCandidate.length; i++) {
				int comparisonResult = -1; // initial value is important in case the following for loop isn't executed because j is too great
				for (; j < transaction.length; j++) {
					comparisonResult = transaction[j] - myCandidate[i];
					if (comparisonResult >= 0) {
						break;
					}
				}
				if (comparisonResult != 0) { // comparisonResult < 0 means the end of the transaction was reached but candidate.get(i) wasn't found
												// comparisonResult > 0 means the transaction doesn't contain candidate.get(i) (, but an element greater than candidate.get(i) )
					break;
				}

				if (i == myCandidate.length - 1) { // if all elements of candidate were found in transaction
					out.collect(new Tuple2<IntArray, Integer>(myCandidateWrapper, 1));
				}
			}
		}
	}

}