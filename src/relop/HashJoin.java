package relop;

/**
 * Hash join: references on 462-463
 */
public class HashJoin extends Iterator {

	private IndexScan left;
	private IndexScan right;
	private int leftCol;
	private int rightCol;
	private HashTableDup hashTable;

	/**
	 * Constructs a join, given the left and right iterators and join predicates
	 * (relative to the combined schema).
	 */
	public HashJoin(Iterator left, Iterator right, int leftCol, int rightCol) {
		
		if(left instanceof FileScan){
		
		}

		if(left instanceof KeyScan){
		
		}
		
		if(left instanceof IndexScan){
		
		}
		
		if(right instanceof FileScan){
		
		}

		if(right instanceof KeyScan){
		
		}
		
		if(right instanceof IndexScan){
		
		}
	}

	/**
	 * Gives a one-line explanation of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		
		throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
		left.restart();
		right.restart();
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
		
		if (right.isOpen())
			return true;

		return false;
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {

		right.close();
		left.close();
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 * 
	 */
	public boolean hasNext() {
		throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException if no more tuples
	 */
	public Tuple getNext() {
		
		throw new UnsupportedOperationException("Not implemented");
	}
	
}
