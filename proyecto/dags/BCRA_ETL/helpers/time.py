from datetime import datetime
import time

def GetNowTime():
    now = datetime.now()
    current_time = now.strftime("%H:%M")
    return current_time

def log_time(message, start_time):
    elapsed_time = time.time() - start_time
    print(f"{message}: {elapsed_time:.2f} segundos")

def start_delay():
    #print("Delay")
    for _ in range(4):
        print("Please wait...", flush=True)
        time.sleep(1)    
    print("\n")