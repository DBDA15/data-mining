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
	
	public static HashSet<HashSet<String>> powerSet(List<String> originalSet) {
		HashSet<HashSet<String>> sets = new HashSet<HashSet<String>>();
        if (originalSet.size() == 0) {
            sets.add(new HashSet<String>());
            return sets;
        }
        //List<Integer> list = new ArrayList<Integer>(originalSet);
        String head = originalSet.get(0);
        //Set<Integer> rest = new HashSet<Integer>(list.subList(1, list.size()));
		List<String> rest = originalSet.size() > 1 ? originalSet.subList(1, originalSet.size()) : new ArrayList<String>(0);
        for (HashSet<String> set : powerSet(rest)) {
        	HashSet<String> newSet = new HashSet<String>();
            newSet.add(head);
            newSet.addAll(set);
            sets.add(newSet);
            sets.add(set);
        }
        return sets;
    }

}
