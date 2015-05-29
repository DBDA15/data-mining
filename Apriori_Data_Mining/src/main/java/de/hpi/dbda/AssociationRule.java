package de.hpi.dbda;

import java.util.HashSet;

public class AssociationRule<T> {

	public HashSet<T> first;
	public HashSet<T> last;
	
	public AssociationRule(HashSet<T> f, HashSet<T> l) {
		first = f;
		last = l;
	}
	
	public void print() {
		System.out.println(first.toString()+"=>"+last.toString());
	}
	
	public String getPrintVersion() {
		return first.toString()+"=>"+last.toString()+"\n";
	}

}
