package de.hpi.dbda.trie;

public class InnerTrieNode implements TrieNode {
	private static final long serialVersionUID = -4004959451511007940L;
	public int[] edgeLabels;
	public TrieNode[] children;

	public InnerTrieNode(int[] edgeLabels, TrieNode[] children) {
		this.edgeLabels = edgeLabels;
		this.children = children;
	}
}
