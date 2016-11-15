import pandas as pd
from mysql.connector import conversion
import mysql
import time
import datetime
import numpy as np


class db_connection():

    config_9niu_stage = { 'user': 'root',
    'host': '192.168.1.93',
    'password': '9niu147258',
    'database': '9niu_stage'}

    config_9niu_bi = { 'user': 'root',
    'host': '192.168.1.93',
    'password': '9niu147258',
    'database': '9niu_bi'}


