from datebase_conntection import db_connection
import mysql
import time
from mysql.connector import conversion
import datetime
import numpy as np
class NumpyMySQLConverter(conversion.MySQLConverter):
    """ A mysql.connector Converter that handles Numpy types """
    def _float32_to_mysql(self, value):
        return float(value)
    def _float64_to_mysql(self, value):
        return float(value)
    def _int32_to_mysql(self, value):
        return int(value)
    def _int64_to_mysql(self, value):
        return int(value)