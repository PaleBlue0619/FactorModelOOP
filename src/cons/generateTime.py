import json5
import dolphindb as ddb # 交易日历
import pandas as pd
from typing import List, Dict

import json5
import dolphindb as ddb
import pandas as pd
from typing import List, Dict


def generateTime(session: ddb.session, startDate: str, endDate: str,
                 callBackPeriod: int = 5, windowSize: int = 10) \
        -> Dict[str, List[str]]:
    """
    滚动选取过去K个交易日 -> 每次往前挪动长度为window的窗口
    """
    # 获取交易日历
    startDate = pd.Timestamp(startDate).strftime("%Y.%m.%d")
    endDate = pd.Timestamp(endDate).strftime("%Y.%m.%d")

    data = session.run(f"""
        dateList = getMarketCalendar("CFFEX", {startDate}, {endDate});
        table(dateList as tradeDate);
    """)

    dateList = data["tradeDate"].apply(pd.Timestamp).tolist()
    resultDict = {}

    # 滚动窗口
    for i in range(0, len(dateList), callBackPeriod):
        endIdx = min(i + windowSize, len(dateList))
        startIdx = max(0, endIdx - windowSize)
        windowDates = dateList[startIdx:endIdx]

        if len(windowDates) == windowSize:  # 只保存完整窗口
            resultDict[windowDates[-1].strftime("%Y.%m.%d")] = [
                windowDates[0].strftime("%Y.%m.%d"),
                windowDates[-1].strftime("%Y.%m.%d")
            ]

    return resultDict


if __name__ == "__main__":
    timeDict = generateTime(ddb.session("localhost", 8848, "admin", "123456"),
                            "2021-01-01", "2026-02-13",
                            callBackPeriod=5, windowSize=10)
    with open(r"D:\DolphinDB\Project\FactorModel_v1\src\cons\time.json5", "w", encoding="utf-8") as f:
        json5.dump(timeDict, f)