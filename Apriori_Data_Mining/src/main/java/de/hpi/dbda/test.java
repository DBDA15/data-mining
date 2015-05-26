package de.hpi.dbda;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import de.hpi.dbda.IntArray;


public class test {

	public static HashSet<HashSet<Integer>> powerSet(IntArray originalSet) {
		HashSet<HashSet<Integer>> sets = new HashSet<HashSet<Integer>>();
        if (originalSet.length() == 0) {
            sets.add(new HashSet<Integer>());
            return sets;
        }
        //List<Integer> list = new ArrayList<Integer>(originalSet);
        Integer head = originalSet.head();
        //Set<Integer> rest = new HashSet<Integer>(list.subList(1, list.size()));
        IntArray rest = originalSet.rest();
        for (HashSet<Integer> set : powerSet(rest)) {
        	HashSet<Integer> newSet = new HashSet<Integer>();
            newSet.add(head);
            newSet.addAll(set);
            sets.add(newSet);
            sets.add(set);
        }
        return sets;
    }
	
	public static void main(String[] args) {
		int[] i = {1,2,3};
		ArrayList<AssociationRule> ar = new ArrayList();
		IntArray set = new IntArray(i);
		
		HashSet<HashSet<Integer>> pow = powerSet(set);
		pow.remove(new HashSet<Integer>());
		pow.remove(set.valueSet());
		for (HashSet<Integer> s : pow) {
			HashSet<Integer> o = set.valueSet();
			o.removeAll(s);
			AssociationRule asr = new AssociationRule(s, o);
			ar.add(asr);
			asr.print();
		}
		/*for (Set<Integer> s : pow) {
			HashSet newSet = HashSet(set.value);
			newSet.removeAll(s);
			System.out.println(newSet.toString()+"=>"+s);

		}*/

	}

}
