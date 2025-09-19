import pandas as pd 
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime
from config import *




#-----------------------Connection-----------------------------------#
def connection_to_database():
    return create_engine(f"mysql+pymysql://{db_user}@{db_host}/{db_name}")

