import time
from datetime import *

class DatetimeKeyGenerator:
    _ROW_KEY_BASE_DATETIME_FORMATTER = '%Y%m%d%H%M%S0000000'

    def generateTimeRowKeyStart(self,datetimeString,datetimeFormat = '%Y-%m-%d %H:%M:%S'):
        rowkey = datetime.strptime(datetimeString,datetimeFormat).strftime(self._ROW_KEY_BASE_DATETIME_FORMATTER)

        return rowkey

