import schedule
import time

def job(name):
    print("I'm working...",name)

schedule.every(10).minutes.do(job,name='schedule.every(10).minutes')
schedule.every().hour.do(job,name='schedule.every().hour')
schedule.every().day.at("10:30").do(job,name='schedule.every().day')
schedule.every(5).to(10).minutes.do(job,name='schedule.every(5).to(10).minutes')
schedule.every().monday.do(job,name='schedule.every().monday')
schedule.every().wednesday.at("13:15").do(job,name='schedule.every().wednesday.at("13:15")')
schedule.every().minute.at(":17").do(job,name='schedule.every().minute.at(":17")')

while True:
    schedule.run_pending()
    time.sleep(1)