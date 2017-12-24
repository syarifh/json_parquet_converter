import json
import types
import argparse
import logging
import time
import re
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import pyspark.sql.types as pst
from pyspark.sql import Row
import configparser, os
import subprocess
sc = SparkContext('local')
spark = SparkSession(sc)

class ConvertClass():
    def infer_schema(self,rec):
        """infers dataframe schema for a record. Assumes every dict is a Struct, not a Map"""
        if isinstance(rec, dict):
            return pst.StructType([pst.StructField(key, self.infer_schema(value), True)
                                   for key, value in sorted(rec.items())])
        elif isinstance(rec, list):
            if len(rec) == 0:
                raise ValueError("can't infer type of an empty list")
            elem_type = self.infer_schema(rec[0])
            for elem in rec:
                this_type = self.infer_schema(elem)
                if elem_type != this_type:
                    raise ValueError("can't infer type of a list with inconsistent elem types")
            return pst.ArrayType(elem_type)
        else:
            return pst._infer_type(rec)

    def run(self, argv=None):
        print(time.strftime("%b %d %Y %H:%M:%S", time.gmtime()))
        parser = argparse.ArgumentParser(description='Process join files.')
        parser.add_argument(
            '--mode',
            required=True,
            help='pick one: sparktojson, jsontoparquet".')
        parser.add_argument(
            '--input',
            required=True,
            help='input file json".')
        parser.add_argument(
            '--output',
            required=True,
            help='output parquet".')
        parser.add_argument(
            '--schema_dict',
            required=True,
            help='schema from dummy data in json (dict)".')
        parser.add_argument(
            '--partition',
            required=False,
            help='partition key".')
        known_args, pipeline_args = parser.parse_known_args(argv)

        subprocess.call('hostname', shell=True)
        subprocess.call('pwd', shell=True)
        subprocess.call('whoami', shell=True)
        dummy_dict_file = 'tmp/dummy_dict.json'
        subprocess.call('mkdir tmp',shell=True)
        subprocess.call('ls -ltr', shell=True)
        subprocess.call('gsutil cp '+ known_args.schema_dict +' '+ dummy_dict_file, shell=True)
        subprocess.call('ls -ltr', shell=True)

        input = known_args.input
        output =known_args.output
        schema = dummy_dict_file
        partition = known_args.partition
        if(known_args.mode == 'jsontoparquet'):
            self.jsontoparquet(input,output,schema,partition)
        else:
            self.parquettojson(input, output, schema)

    def jsontoparquet(self,input,output,schema,partition):
        df = spark.read.json(input)
        df.printSchema()

        prototype= json.loads(open(schema).read())

        df_schm = spark.createDataFrame(df.rdd, self.infer_schema(prototype), spark)
        df_schm.show()
        if partition != '':
            df.write.partitionBy(partition).parquet(output)
        else:
            df_schm.write.parquet(output)
        print(time.strftime("%b %d %Y %H:%M:%S", time.gmtime()))

    def parquettojson(self, input, output, schema):
        df = spark.read.parquet(input)
        df.printSchema()

        prototype = json.loads(open(schema).read())
        self.infer_schema(prototype)

        df_schm = spark.createDataFrame(df.rdd, self.infer_schema(prototype), spark)
        df_schm.show()
        df_schm.write.json(output)
        print(time.strftime("%b %d %Y %H:%M:%S", time.gmtime()))

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    # run()
    convert = ConvertClass()
    convert.run()
