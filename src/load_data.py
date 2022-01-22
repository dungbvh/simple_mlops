# read the data from data source
# save it in the data/raw for further process

import os
from posixpath import sep 
import re
from get_data import read_params,get_data
import argparse


def standardize_name(cname):
    cname = re.sub(r'[-\.]', ' ', cname)
    cname = cname.strip().lower()
    cname = re.sub(r'\s+', '_', cname)
    return cname


def load_and_save(config_path):
    config = read_params(config_path=config_path)
    df = get_data(config_path)
    df.columns = df.columns.map(standardize_name)
    raw_data_path = config["load_data"]["raw_dataset_csv"]
    df.to_csv(raw_data_path, sep=",", index=False, encoding='utf-8')


if __name__=="__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--config", default="params.yaml")
    parsed_args = args.parse_args()
    load_and_save(config_path=parsed_args.config)
