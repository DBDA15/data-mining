package de.hpi.dbda.triejobs;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.functions.FunctionAnnotation.ReadFields;
import org.apache.flink.api.java.tuple.Tuple2;

import de.hpi.dbda.IntArray;

@ReadFields("")
public class MyKeySelector implements KeySelector<Tuple2<IntArray,Integer>,Integer>{
	private static final long serialVersionUID = 3910614534178503617L;

	public Integer getKey(Tuple2<IntArray, Integer> t) {
		return 42;
	}

}
