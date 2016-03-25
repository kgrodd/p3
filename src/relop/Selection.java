package relop;


/**
 * The selection operator specifies which tuples to retain under a condition; in
 * Minibase, this condition is simply a set of independent predicates logically
 * connected by OR operators.
 */
public class Selection extends Iterator {
	private Iterator iter;
	private Predicate[] preds;
	private Tuple currTuple = null;

  /**
   * Constructs a selection, given the underlying iterator and predicates.
   */
  public Selection(Iterator iter, Predicate... preds) {
  	this.shema = iter.schema;
    this.iter=iter;
    this.preds=preds;
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    	System.out.println("Selection- Depth: " + depth);
    	iter.explain(depth + 1);
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
  	currTuple = null;
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
  	if(!iter.hasNext())
  		return false;
  
    else{
    	currTuple = iter.getNext();
    	for(int i = 0; i < preds.length; i++){
    		if(!preds[i].evaluate(currTuple))
    			return hasNext();
    		
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
	return currTuple;
  }

} // public class Selection extends Iterator
