package relop;

import global.RID;
import heap.HeapFile;
import heap.HeapScan;

/**
 * Wrapper for heap file scan, the most basic access method. This "iterator"
 * version takes schema into consideration and generates real tuples.
 */
public class FileScan extends Iterator {
	private HeapFile hf = null;
	private HeapScan hs = null;
	private RID currRID = new RID();
	private Tuple currTuple = null;

  /**
   * Constructs a file scan, given the schema and heap file.
   */
  public FileScan(Schema schema, HeapFile file) {
    this.schema = schema;
	this.hf = file;
	this.hs = this.hf.openScan();
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
	this.indent(depth);
    System.out.println("FileScan Iterator!");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	if(this.isOpen()) {
		this.close();
	}
    this.hs = this.hf.openScan();
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
	byte [] arr = this.hs.getNext(this.currRID);
	if(arr == null)
		throw new IllegalStateException("Out of tuples");
	this.currTuple = new Tuple(this.schema, arr);

	return (this.currTuple);
  }

  /**
   * Gets the RID of the last tuple returned.
   */
  public RID getLastRID() {
    return this.currRID;
  }
  

  	public HeapFile getFile() {
		return this.hf;
	}

} // public class FileScan extends Iterator
