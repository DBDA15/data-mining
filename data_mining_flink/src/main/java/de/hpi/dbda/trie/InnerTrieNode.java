package de.hpi.dbda.trie;

import java.io.Serializable;

public class InnerTrieNode implements Serializable{
	private static final long serialVersionUID = -4004959451511007940L;
	public int[] edgeLabels;
	public int candidateID = -1;
	public InnerTrieNode[] children;

	public InnerTrieNode(int[] edgeLabels, InnerTrieNode[] children) {
		this.edgeLabels = edgeLabels;
		this.children = children;
	}
	public InnerTrieNode(int index, int[] edgeLabels, InnerTrieNode[] children) {
		this.edgeLabels = edgeLabels;
		this.children = children;
		this.candidateID = index;
	}
}
