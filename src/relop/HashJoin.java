package relop;

import index.HashIndex;
import heap.HeapFile;
import global.SearchKey;
import global.RID;

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
		this.nextTuple = new Tuple(this.schema);
		
		HeapFile file = new HeapFile(null);
		HashIndex index = new HashIndex(null);
		
		
		if(l instanceof FileScan){
			Tuple tempTuple = new Tuple(schema);
			while(l.hasNext()){
				tempTuple = l.getNext();
				RID rid = file.insertRecord(tempTuple.getData());
				index.insertEntry(new SearchKey(tempTuple.getField(leftCol)), rid);
			}
			IndexScan indexScan = new IndexScan(schema, index, file);
			this.left = indexScan;
		}

		else if(l instanceof KeyScan){
			
		}
		
		else if(l instanceof IndexScan){
			this.left = (IndexScan)l;
		}
		
		if(r instanceof FileScan){
			Tuple tempTuple = new Tuple(schema);
			while(l.hasNext()){
				tempTuple = r.getNext();
				RID rid = file.insertRecord(tempTuple.getData());
				index.insertEntry(new SearchKey(tempTuple.getField(rightCol)), rid);
			}
			IndexScan indexScan = new IndexScan(schema, index, file);
			this.left = indexScan;
		}

		else if(r instanceof KeyScan){
			
		}
		
		else if(r instanceof IndexScan){
			this.right = (IndexScan)r;
		}
		
		this.schema = Schema.join(l.schema, r.schema);

	}

	/**
	 * Gives a one-line explanation of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		
		this.indent(depth);
		System.out.println("HashJoin!");
		left.explain(depth+1);
		right.explain(depth+1);
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
		System.out.println("left.hasNext() ......." + left.hasNext());
		if(!left.hasNext() || !right.hasNext())
			return false; 
		

System.out.println("not empty!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
		int leftBucket = left.getNextHash();
		int rightBucket = right.getNextHash();
		SearchKey leftKey;
		SearchKey rightKey;
		Tuple [] tupleArray;
		
		HashTableDup hashTable = new HashTableDup();
		
		
		while(true){
			while(leftBucket == left.getNextHash()){
				leftTuple = left.getNext();
				leftTuple.print();
				leftKey = new SearchKey(leftTuple.getField(leftCol));
				hashTable.add(leftKey,leftTuple);
			}
			while(leftBucket == rightBucket){
				rightTuple = right.getNext();
				leftTuple.print();
				rightTuple.print();
				rightKey = new SearchKey(rightTuple.getField(rightCol));
				tupleArray = hashTable.getAll(rightKey);
				rightBucket = right.getNextHash();
				if(tupleArray == null){
					continue;
				}
				nextTuple = Tuple.join(rightTuple, tupleArray[position], this.schema);
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
