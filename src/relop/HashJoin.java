package relop;

import index.HashIndex;
import heap.HeapFile;
import global.SearchKey;

/**
 * Hash join: references on 462-463
 */
public class HashJoin extends Iterator {

	private IndexScan left;
	private IndexScan right;
	private int leftCol;
	private int rightCol;
	int position = 0;
	Tuple nextTuple;
	Tuple leftTuple;
	Tuple rightTuple;

	

	/**
	 * 	Constructs a join, given the left and right iterators and what columns to check join on
	 *
	 *	Other scan formats for reference
	 *  IndexScan(Schema schema, HashIndex index, HeapFile file)
	 *	FileScan(Schema schema, HeapFile file)
	 *	KeyScan(Schema schema, HashIndex index, SearchKey key, HeapFile file)
	 */
	public HashJoin(Iterator l, Iterator r, int leftCol, int rightCol) {
		
		this.leftCol = leftCol;
		this.rightCol = rightCol;
		this.schema = Schema.join(left.schema, right.schema);
		this.nextTuple = new Tuple(this.schema);
		this.leftTuple = left.getNext();
		this.rightTuple = right.getNext();
		/*
		
		if(l instanceof FileScan){
			FileScan tempFileScan = (FileScan)l;
			HeapFile tempHeap = tempFileScan.getHeapFile();
			HashIndex tempHash = new HashIndex(tempFileScan.toString());
			IndexScan tempScan = new IndexScan(schema, tempHash , tempHeap);
			this.left = tempScan;
		}

		else if(l instanceof KeyScan){
			KeyScan tempKeyScan = (KeyScan)l;
			HashIndex tempHash = tempKeyScan.getHashIndex();
			HeapFile tempHeap = tempKeyScan.getHeapFile();
			IndexScan tempScan = new IndexScan(schema, tempHash , tempHeap);
			this.left = tempScan;
		}
		
		else if(l instanceof IndexScan){
			this.left = (IndexScan)l;
		}
		
		if(r instanceof FileScan){
			FileScan tempFileScan = (FileScan)r;
			HashIndex tempHash = new HashIndex(null);
			HeapFile tempHeap = tempFileScan.getHeapFile();
			IndexScan tempScan = new IndexScan(schema, tempHash , tempHeap);
			this.right = tempScan;
		}

		else if(r instanceof KeyScan){
			KeyScan tempKeyScan = (KeyScan)r;
			HashIndex tempHash = tempKeyScan.getHashIndex();
			HeapFile tempHeap = tempKeyScan.getHeapFile();
			IndexScan tempScan = new IndexScan(schema, tempHash , tempHeap);
			this.right = tempScan;	
		}
		
		else if(r instanceof IndexScan){
			this.right = (IndexScan)r;
		}
		
		*/

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
		
		if (left.isOpen())
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
		if(!left.hasNext())
			return false; 
		
		int leftBucket = left.getNextHash();
		int rightBucket = right.getNextHash();
		SearchKey leftKey;
		SearchKey rightKey;
		Tuple [] tupleArray;
		
		HashTableDup hashTable = new HashTableDup();
		
		
		while(true){
			while(leftBucket == left.getNextHash()){
				leftTuple = left.getNext();
				leftKey = new SearchKey(leftTuple.getField(leftCol));
				hashTable.add(leftKey,leftTuple);
			}
			while(leftBucket == rightBucket){
				rightTuple = right.getNext();
				rightKey = new SearchKey(rightTuple.getField(rightCol));
				tupleArray = hashTable.getAll(rightKey);
				rightBucket = right.getNextHash();
				nextTuple = Tuple.join(leftTuple, tupleArray[position], this.schema);
				if(tupleArray.length == 0){
					return false;
				}
				return true;
			}
			leftBucket = left.getNextHash();
			rightBucket = right.getNextHash();
			hashTable.clear();
		}
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException if no more tuples
	 */
	public Tuple getNext() {
		position++; 
		return nextTuple;
	}
	
}
