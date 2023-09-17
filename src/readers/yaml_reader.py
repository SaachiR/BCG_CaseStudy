import yaml

def read_yaml_from_path(file_path):
    """
    Read the configuartion file in yaml format.

    Parameters:
    ----------
      file_path: path to configuration file

    Returns:
    ---------------------
      configuration details dictionary
    """
    try:
        with open(file_path, "r") as f:
            return yaml.safe_load(f)
    except Exception as e:
        print("Error : " + str(e))