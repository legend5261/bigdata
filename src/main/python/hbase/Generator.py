import time
from datetime import *

class Generator:
    ROW_KEY_BASE_DATETIME = '2010-01-01 00:00:00'
    ROW_KEY_BASE_DATETIME_FORMATTER = '%Y-%m-%d %H:%M:%S'
    ROW_KEY_INCREMENT_PART_LENGTH_DEFAULT = 8

    def generateTimeRowKeyStart(self,datetimeString,datetimeFormat = '%Y-%m-%d %H:%M:%S'):
        baseTime = datetime.strptime(self.ROW_KEY_BASE_DATETIME,self.ROW_KEY_BASE_DATETIME_FORMATTER)
        iptTime = datetime.strptime(datetimeString,datetimeFormat)

        seconds = iptTime.hour * 3600 + iptTime.minute * 60 + iptTime.second
        timedelta = iptTime - baseTime
        timeDiff = timedelta.days * 100000 + seconds

        rowKey = hex(timeDiff)[2:]
        if len(rowKey) < 8:
            rowKey = rowKey + ''.zfill(8 - len(rowKey))

        rowKey = rowKey + ''.zfill(self.ROW_KEY_INCREMENT_PART_LENGTH_DEFAULT)

        return rowKey


