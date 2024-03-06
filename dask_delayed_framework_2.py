"""
a framework for not exposing dask delayed  and write a warpper around it so we can chunk the data which is a dataframe 
"""
import dask
import pandas as pd

def my_delayed(func):
  @dask.delayed
  def wrapped(*args, **kwargs):
    return func(*args, **kwargs)
  return wrapped

def chunked_delayed(func):
  def wrapped(*args, **kwargs):
    chunk_size = kwargs.pop('chunk_size', None)
    if isinstance(args[0], pd.DataFrame) and chunk_size is not None:
      chunks = [args[0][i:i+chunk_size] for i in range(0, len(args[0]), chunk_size)]
      delayed_results = [my_delayed(func)(chunk, *args[1:], **kwargs) for chunk in chunks]
      return dask.compute(*delayed_results)
    else:
      return my_delayed(func)(*args, **kwargs)
  return wrapped

# Example usage
@chunked_delayed
def process_data(df, column):
  # Perform some computation on the DataFrame
  return df[column].mean()

# Create a sample DataFrame
df = pd.DataFrame({'A': [1, 2, 3, 4, 5], 'B': [6, 7, 8, 9, 10]})

# Chunk size for processing the DataFrame
chunk_size = 2

# Compute the mean of column 'A' in chunks
result = process_data(df, 'A', chunk_size=chunk_size)
print(result)  # Output: [2.5, 4.5]