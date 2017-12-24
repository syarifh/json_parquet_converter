import subprocess
import argparse
import configparser, os
import logging
import time

class SubProc():
    def run(self, argv=None):
        configParser = configparser.RawConfigParser()
        configFilePath = r'config/cluster.conf'
        configParser.read(configFilePath)
        cluster = configParser.get('cluster_conf', 'cluster')
        region = configParser.get('cluster_conf', 'region')
        schema = configParser.get('cluster_conf','schema_bucket')

        parser = argparse.ArgumentParser(description='Process execute.')
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
        caller = "gcloud dataproc jobs submit pyspark" + " --cluster " + cluster + " --region " + region + " source/converter.py" + " -- --input " + known_args.input + " --output " + known_args.output + " --schema_dict " + known_args.schema_dict + " --mode " + known_args.mode

        if known_args.partition is not None:
            caller = caller + " --partition " + known_args.partition

        print(caller)
        subprocess.call(caller,shell=True)

if __name__ == '__main__':
    # run()
    sp = SubProc()
    sp.run()