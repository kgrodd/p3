package relop;

/**
 * The projection operator extracts columns from a relation; unlike in
 * relational algebra, this operator does NOT eliminate duplicate tuples.
 */
public class Projection extends Iterator {
	private Iterator iter;
	private Integer[] fields;
	private int fldcnt;
	
  /**
   * Constructs a projection, given the underlying iterator and field numbers.
   */
  public Projection(Iterator iter, Integer... fields) {
    this.iter=iter;
    this.fields=fields;
    this.fldcnt=fields.length;
	this.schema=new Schema(fldcnt);
	for(int i = 0; i < fldcnt; i++){
		if(fields[i] == i){
			schema.initField(i, iter.schema.fieldType(i), iter.schema.fieldLength(i), iter.schema.fieldName(i));
		}
	}
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    System.out.println("Projectinon - Depth: " + depth);
    iter.explain(depth + 1);
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    iter.restart();
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    if (iter.isOpen())
		return true;
	return false;
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
	this.iter.close();
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    return iter.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    //Tuple next = itter.getNext();
    //Tuple format = 
  	return iter.getNext();
  }

} // public class Projection extends Iterator
