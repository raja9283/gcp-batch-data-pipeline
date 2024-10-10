import os
import json
from utils import utils

utils.load_env()
print(os.getenv('CLUSTER_NAME'))
