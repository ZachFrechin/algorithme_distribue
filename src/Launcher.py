from time import sleep
from Process import Process

def launch(nbProcess, runningTime=10):
    processes = []

    for i in range(nbProcess - 1):
        processes = processes + [Process("P"+str(i), nbProcess)]

    sleep(runningTime)

    processes.append(Process("P" + str(nbProcess - 1), nbProcess))

    sleep(runningTime)

    for p in processes:
        p.stop()

    for p in processes:
        p.waitStopped()
        

if __name__ == '__main__':

    #bus = EventBus.getInstance()
    
    launch(nbProcess=3, runningTime=10)

    #bus.stop()
