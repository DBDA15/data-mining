package de.hpi.dbda;

import java.util.HashSet;
import java.util.Set;

public class AssociationRule {

	public HashSet<Integer> first;
	public HashSet<Integer> last;
	
	public AssociationRule(HashSet<Integer> f, HashSet<Integer> l) {
		first = f;
		last = l;
	}
	
	public void print() {
		System.out.println(first.toString()+"=>"+last.toString());
	}

}
