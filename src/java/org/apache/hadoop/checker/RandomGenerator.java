package org.apache.hadoop.checker;

public interface RandomGenerator<T> {
  public T next();
  public T expectedValue(T span);
  public T[] next(long n, T span);
}
