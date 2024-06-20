from datetime import datetime

def GetNowTime():
    now = datetime.now()
    current_time = now.strftime("%H:%M")
    return current_time