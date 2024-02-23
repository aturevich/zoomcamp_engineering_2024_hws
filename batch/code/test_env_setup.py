import os
import subprocess
import re
from pyspark import __version__ as pyspark_version


# Environment variables to check
env_vars = ['JAVA_HOME', 'HADOOP_HOME', 'SPARK_HOME']
# Keywords to search for in the PATH
keywords = ['java', 'spark', 'hadoop']

def check_env_vars(env_vars):
    for var in env_vars:
        value = os.getenv(var)
        if value:
            print(f"{var} is set: {value}")
        else:
            print(f"{var} is not set")

def search_keywords_in_path(keywords):
    path_env = os.getenv('PATH')
    paths = path_env.split(os.pathsep)
    found_paths = {keyword: set() for keyword in keywords}  # Use set to avoid duplicates

    for path in paths:
        for keyword in keywords:
            if keyword.lower() in path.lower():
                found_paths[keyword].add(path)  # Add path to the set

    for keyword, paths in found_paths.items():
        if paths:
            print(f"Found '{keyword}' in PATH at:")
            for path in sorted(paths):  # Sort paths for consistent ordering
                print(f"  - {path}")
        else:
            print(f"'{keyword}' not found in PATH")

def get_spark_version_from_system():
    spark_home = os.getenv('SPARK_HOME')
    if spark_home:
        spark_submit_cmd = os.path.join(spark_home, "bin", "spark-submit.cmd")  # Adjusted for Windows
        if not os.path.isfile(spark_submit_cmd):
            print(f"Spark submit command not found at {spark_submit_cmd}")
            return None
        try:
            # Adjusted command execution for Windows
            result = subprocess.run([spark_submit_cmd, "--version"], capture_output=True, text=True, shell=True)
            version_output = result.stderr  # spark-submit --version writes to stderr
            version_match = re.search(r"version (\d+\.\d+\.\d+)", version_output)
            if version_match:
                return version_match.group(1)
        except Exception as e:
            print(f"Error obtaining Spark version from system: {e}")
    return None

def compare_versions(spark_version, pyspark_version):
    if spark_version and spark_version == pyspark_version:
        print(f"PySpark version matches the installed Spark version.")
    else:
        print(f"Mismatch found: PySpark version is not matheching Spark version.")


if __name__ == "__main__":
    print("Checking environment variables:")
    check_env_vars(env_vars)
    print("\nSearching keywords in PATH:")
    search_keywords_in_path(keywords)

    spark_version = get_spark_version_from_system()
    if spark_version:
        print(f"Found Spark version!")
    else:
        print("Spark version could not be determined.")
    
    print(f"Found PySpark!")
    compare_versions(spark_version, pyspark_version)