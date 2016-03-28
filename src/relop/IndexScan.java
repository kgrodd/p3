package relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import index.BucketScan;
/**
 * Wrapper for bucket scan, an index access method.
 */
public class IndexScan extends Iterator {
	private HeapFile hf = null;
	private HashIndex hi = null;
	private BucketScan bs = null;
	private SearchKey sk = null;

  /**
   * Constructs an index scan, given the hash index and schema.
   */
  public IndexScan(Schema schema, HashIndex index, HeapFile file) {
  	this.schema = schema;
	this.hf = file;
	this.hi = index;
	this.bs = this.hi.openScan();
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
	this.indent(depth);
	System.out.println("IndexScan!");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    if(this.isOpen())
		this.close();
	this.bs = this.hi.openScan();
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
	return (this.bs != null ? true : false);
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
  	this.bs.close();
	this.bs = null;
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
	this.hi.printSummary();
	System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" +this.hi.toString());
    return this.bs.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	RID r = this.bs.getNext();
	if(r == null)
		throw new IllegalStateException("Out of tuples"); 
	return (new Tuple(this.schema, this.hf.selectRecord(r)));
  }

  /**
   * Gets the key of the last tuple returned.
   */
  public SearchKey getLastKey() {
    return this.bs.getLastKey();
  }

  /**
   * Returns the hash value for the bucket containing the next tuple, or maximum
   * number of buckets if none.
   */
  public int getNextHash() {
    return this.bs.getNextHash();
  }

} // public class IndexScan extends Iterator
