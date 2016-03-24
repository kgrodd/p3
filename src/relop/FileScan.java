package relop;

import global.RID;
import heap.HeapFile;

/**
 * Wrapper for heap file scan, the most basic access method. This "iterator"
 * version takes schema into consideration and generates real tuples.
 */
public class FileScan extends Iterator {
	private HeapFile hf = null;
	private HeapScan hs = null;
	private RID currRID = null;
	private TUPLE currTuple = null;

  /**
   * Constructs a file scan, given the schema and heap file.
   */
  public FileScan(Schema schema, HeapFile file) {
    this.schema = schema;
	this.hf = file;
	this.hs = this.hf.openscan();
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    System.out.println("FileScan Iterator! depth: " + depth);
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	if(this.isOpen()) {
		this.close();
	}
    this.hs = this.hf.openscan();
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    return (this.hs ? true : false);
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
	if(!this.hasNext())
		throw new IllegalStateException("No more Tuples");
	return (this.currTuple = new Tuple(this.schema, this.hs.getNext(this.currRID)));
  }

  /**
   * Gets the RID of the last tuple returned.
   */
  public RID getLastRID() {
    return this.currRID;
  }

} // public class FileScan extends Iterator
