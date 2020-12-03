import os
import yaml

ROOT_DIR = os.path.dirname(__file__)

with open(os.path.join(ROOT_DIR, 'conf/database.yaml')) as f:
    DBCFG = yaml.load(f, Loader=yaml.SafeLoader) 
