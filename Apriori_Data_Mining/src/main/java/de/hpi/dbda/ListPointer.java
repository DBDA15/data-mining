package de.hpi.dbda;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;

import scala.NotImplementedError;

public class ListPointer implements List{
	private List<String> listElements;
	private static final int hashcode = 1938638499;
	private int ignoreIndex;
	
	public ListPointer(List<String> el, int ignoreIndex) {
		listElements = el;
		this.ignoreIndex=ignoreIndex;
	}
	@Override
	public int hashCode() {
		int hash = hashcode;
		for (int i = 0; i < listElements.size(); i++) {
			if (i == ignoreIndex) {
				continue;
			}
			hash = Objects.hash(hash, listElements.get(i));
		}
		return hash;
	}
	
	@Override
	public boolean equals(Object o) {
		if (o ==  null)
			return false;
		if (o instanceof ListPointer == false) 
			return false;
		ListPointer otherList = (ListPointer)o;
		//if(this.listElements.size())
		int myIndex=0;
		int otherIndex=0;
		for (int i = 0; i < this.listElements.size() - 1; i++) {
			if (myIndex == this.ignoreIndex) {
				myIndex++;
			}
			if (otherIndex == otherList.ignoreIndex) {
				otherIndex++;
			}
			if (!this.listElements.get(myIndex).equals(otherList.listElements.get(otherIndex)))
					return false;
			myIndex++;
			otherIndex++;
		}
		return true;
	}

	public boolean add(Object e) {
		throw new NotImplementedError();
	}
	public void add(int index, Object element) {
		throw new NotImplementedError();
	}
	public boolean addAll(Collection c) {
		throw new NotImplementedError();
	}
	public boolean addAll(int index, Collection c) {
		throw new NotImplementedError();
	}
	public void clear() {
		throw new NotImplementedError();
	}
	public boolean contains(Object o) {
		throw new NotImplementedError();
	}
	public boolean containsAll(Collection c) {
		throw new NotImplementedError();
	}
	public Object get(int index) {
		if(ignoreIndex <= index){
			index++;
		}
		return listElements.get(index);
	}
	public int indexOf(Object o) {
		throw new NotImplementedError();
	}
	public boolean isEmpty() {
		return listElements.size() <= 1;
	}
	public Iterator iterator() {
		throw new NotImplementedError();
	}
	public int lastIndexOf(Object o) {
		throw new NotImplementedError();
	}
	public ListIterator listIterator() {
		throw new NotImplementedError();
	}
	public ListIterator listIterator(int index) {
		throw new NotImplementedError();
	}
	public boolean remove(Object o) {
		throw new NotImplementedError();
	}
	public Object remove(int index) {
		throw new NotImplementedError();
	}
	public boolean removeAll(Collection c) {
		throw new NotImplementedError();
	}
	public boolean retainAll(Collection c) {
		throw new NotImplementedError();
	}
	public Object set(int index, Object element) {
		throw new NotImplementedError();
	}
	public int size() {
		return listElements.size() - 1;
	}
	public List subList(int fromIndex, int toIndex) {
		throw new NotImplementedError();
	}
	public Object[] toArray() {
		throw new NotImplementedError();
	}
	public Object[] toArray(Object[] a) {
		throw new NotImplementedError();
	}
}
