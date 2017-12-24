**Datafile converter** is using to convert json file to parquet and vice versa.
First, the input and output data must be stored in google cloud storage platform.

For convert json file to parquet, follow this step:
1. Define your cluster properties in file config/cluster.conf
2. Run:
    python exec_converter.py --mode jsontoparquet --input gs://<your gs input directory> --output gs://<your gs output directory> --partition <column that you want to use as partition>
    

For convert parquet file to json, follow this step:
1. Define your cluster properties in file config/cluster.conf
2. Define your table schema as a json file on gs directory
3. Run:
    python exec_converter.py --mode jsontoparquet --input gs://<your gs input directory> --output gs://<your gs output directory> --schema_dict gs://<your schema directory and file>
