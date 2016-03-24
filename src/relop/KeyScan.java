package relop;

import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import heap.HashScan;
/**
 * Wrapper for hash scan, an index access method.
 */
public class KeyScan extends Iterator {

	private HeapFile hf;
	private HashIndex hi;
	private SearchKey key;
	private HashScan hs;

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
    return (this.hi != null ? true : false);
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if there are more tuples, false otherwise.
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

} // public class KeyScan extends Iterator
