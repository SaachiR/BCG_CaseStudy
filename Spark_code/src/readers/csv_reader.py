def load_csv_to_df(spark, file_path):
    """
    Read csv file via Dataframe APIs from the specified path.

    Parameters:
    ----------
      spark : spark session
      file_path: path to csv file

    Returns:
    ---------------------
      dataframe
    """
    return spark.read.option("inferSchema", "true").csv(file_path, header=True).cache()
