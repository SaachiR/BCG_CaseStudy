from pyspark.sql import Row

def drop_df_duplicates(input_dataframe,subset=None):
    """
    Drop duplicates row based on the columns subset from the dataframe.

    Parameters:
    ----------
      input_dataframe: input dataframe
      subset: column list

    Returns:
    ---------------------
      dataframe
    """
    return input_dataframe.dropDuplicates(subset)