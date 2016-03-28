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
	Tuple [] rArray = null;
	int leftHash = -1;
	int rightHash = -1;

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

		this.schema = Schema.join(l.schema, r.schema);
		this.nextTuple = new Tuple(this.schema);
		
		HeapFile file = new HeapFile(null);
		HashIndex index = new HashIndex(null);
		
		if(r instanceof IndexScan){
			this.right = (IndexScan)r;
		}
		else if (r instanceof FileScan){
			FileScan rfs = (FileScan)r;
			Tuple tempTuple = new Tuple(r.getSchema());
			while(rfs.hasNext()){
				tempTuple = rfs.getNext();
				RID rid = rfs.getLastRID();
				index.insertEntry(new SearchKey(tempTuple.getField(rightCol)), rid);
			}
			IndexScan indexScan = new IndexScan(rfs.getSchema(), index, file);
			this.right = indexScan;

		}else{
			Tuple tempTuple = new Tuple(r.getSchema());
			while(r.hasNext()){
				tempTuple = r.getNext();
				RID rid = file.insertRecord(tempTuple.getData());
				index.insertEntry(new SearchKey(tempTuple.getField(rightCol)), rid);
			}
			IndexScan indexScan = new IndexScan(r.getSchema(), index, file);
			this.right = indexScan;
		}

		file = new HeapFile(null);
		index = new HashIndex(null);
		if(l instanceof IndexScan){
			this.left = (IndexScan)l;
		}
		else if (l instanceof FileScan){
			FileScan lfs = (FileScan)l;
			Tuple tempTuple = new Tuple(l.getSchema());
			while(lfs.hasNext()){
				tempTuple = lfs.getNext();
				RID rid = lfs.getLastRID();
				index.insertEntry(new SearchKey(tempTuple.getField(leftCol)), rid);
			}
			IndexScan indexScan = new IndexScan(l.getSchema(), index, file);
			this.left = indexScan;

		}else{
			Tuple tempTuple = new Tuple(l.getSchema());
			while(l.hasNext()){
				tempTuple = l.getNext();
				RID rid = file.insertRecord(tempTuple.getData());
				index.insertEntry(new SearchKey(tempTuple.getField(leftCol)), rid);
			}
			IndexScan indexScan = new IndexScan(l.getSchema(), index, file);
			this.left = indexScan;
		}
		
		


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
		if(!getHashesEqual()) {
			return false;
		}

		HashTableDup hashTable = new HashTableDup();

		while(rArray == null || position == 0) {
			SearchKey rightKey = new SearchKey(rightTuple.getField(rightCol));
			hashTable.add(rightKey,rightTuple);

			if(!right.hasNext() || right.getNextHash() != rightHash) {
				rArray = hashTable.getAll(rightKey);			
				break;
			}

			rightTuple = right.getNext();
		}
		//System.out.println("pos : " + position + "rar len : " + rArray.length);
		//System.out.println("rhash : " + rightHash + " : lHash : " + leftHash);
		for(;position < rArray.length;position++) {
			nextTuple = Tuple.join(leftTuple, rArray[position] , this.schema);
		}

		if(position == rArray.length)
			
		nextTuple.print();
		return true;
	}


	public boolean getHashesEqual() {
		while(true) {
			if(rightHash < leftHash || rightHash == -1) {
				if(right.hasNext()) {
					rightHash = right.getNextHash();
					rightTuple = right.getNext();
					continue;
				}
				return false;
			} else if (leftHash < rightHash || (rArray != null && position == rArray.length)) {
				if(left.hasNext()) {
					position = 0;
					leftHash = left.getNextHash();
					leftTuple = left.getNext();
					continue;
				}
				return false;
			}
			return true;
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
