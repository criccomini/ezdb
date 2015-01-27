package ezdb;

//@NotThreadSafe
public abstract class LazyGetter<T> {
	
	private T value;
	private boolean valueShouldBeNull;
	
	public final T get(){
		if(valueShouldBeNull){
			return null;
		}else{
			if(value == null){
				value = internalGet();
				if(value == null){
					//do not call maybe slow serde again
					valueShouldBeNull = true;
				}
			}
			return value;
		}
	}

	protected abstract T internalGet();
	
}
