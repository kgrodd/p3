package relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import index.HashScan;
/**
 * Wrapper for hash scan, an index access method.
 */
public class KeyScan extends Iterator {

	private HeapFile hf = null;
	private HashIndex hi = null;
	private SearchKey key = null;
	private HashScan hs = null;

  /**
   * Constructs an index scan, given the hash index and schema.
   */
  public KeyScan(Schema schema, HashIndex index, SearchKey key, HeapFile file) {
    this.schema = schema;
	this.hf = file;
	this.hi = index;
	this.key = key;
	this.hs = this.hi.openScan(this.key);
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
	this.indent(depth);
	System.out.println("KeyScan!");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    if(this.isOpen()){
		this.close();
	}
	this.hs = this.hi.openScan(this.key);
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    return (this.hs != null ? true : false);
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
    this.hs.close();
	this.hs = null;
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    return this.hs.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {	
	RID r = this.hs.getNext();
	if(r == null)
		throw new IllegalStateException("Out of tuples"); 

	return (new Tuple(this.schema, this.hf.selectRecord(r)));
  }
  
} // public class KeyScan extends Iterator
