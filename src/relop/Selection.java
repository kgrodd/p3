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
	int pos;
	int depth;

  /**
   * Constructs a selection, given the underlying iterator and predicates.
   */
  public Selection(Iterator iter, Predicate... preds) {
    this.iter=iter;
    this.preds=preds;
    this.pos=0;
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
  	pos=0;
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
    	increment();
    	if(preds[pos].evaluate(currTuple))
    		return true;
    	else
    		return hasNext();
    }
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
  	if(!hasNext())
		throw new IllegalStateException("No more Tuples");
  
    else{
    	currTuple = iter.getNext();
    	increment();
    	if(preds[pos].evaluate(currTuple))
    		return currTuple;
    	else
    		return getNext();
    }
  }
  
  public void increment(){
  	if(pos != 0){
  		pos++;
  	}
  }

} // public class Selection extends Iterator
