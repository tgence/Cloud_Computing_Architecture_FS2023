import docker
import subprocess
import time
import psutil

from datetime import datetime
from enum import Enum
import urllib.parse

LOG_STRING = "{timestamp} {event} {job_name} {args}"
POINTER = 0

class Job(Enum):
    SCHEDULER = "scheduler"
    MEMCACHED = "memcached"
    BLACKSCHOLES = "blackscholes"
    CANNEAL = "canneal"
    DEDUP = "dedup"
    FERRET = "ferret"
    FREQMINE = "freqmine"
    RADIX = "radix"
    VIPS = "vips"


class SchedulerLogger:
    def __init__(self):
        start_date = datetime.now().strftime("%Y%m%d_%H%M%S")

        self.file = open(f"log{start_date}.txt", "w")
        self._log("start", "scheduler")
        self._log("start", "memcached")

    def _log(self, event: str, job_name: Job, args: str = "") -> None:
        self.file.write(
            LOG_STRING.format(timestamp=datetime.now().isoformat(), event=event, job_name=job_name,
                              args=args).strip() + "\n")

    def job_start(self, job: Job, initial_cores, initial_threads: int) -> None:
        assert job != Job.SCHEDULER, "You don't have to log SCHEDULER here"

        self._log("start", job, "["+(",".join(str(i) for i in initial_cores))+"] "+str(initial_threads))

    def job_end(self, job: Job) -> None:
        assert job != Job.SCHEDULER, "You don't have to log SCHEDULER here"

        self._log("end", job)

    def update_cores(self, job: Job, cores) -> None:
        assert job != Job.SCHEDULER, "You don't have to log SCHEDULER here"

        self._log("update_cores", job, "["+(",".join(str(i) for i in cores))+"]")

    def job_pause(self, job: Job) -> None:
        assert job != Job.SCHEDULER, "You don't have to log SCHEDULER here"

        self._log("pause", job)

    def job_unpause(self, job: Job) -> None:
        assert job != Job.SCHEDULER, "You don't have to log SCHEDULER here"

        self._log("unpause", job)

    def custom_event(self, job:Job, comment: str):
        self._log("custom", job, urllib.parse.quote_plus(comment))

    def end(self) -> None:
        self._log("end", Job.SCHEDULER)
        self.file.flush()
        self.file.close()

logger = SchedulerLogger()

client = docker.from_env()

parsec_jobs = [
    {
        "name": "radix",
        "image": "anakli/cca:splash2x_radix",
        "command": "./bin/parsecmgmt -a run -p splash2x.radix -i native -n 4",
        "cpus": "0,1,2,3"
    },
    {
        "name": "vips",
        "image": "anakli/cca:parsec_vips",
        "command": "./bin/parsecmgmt -a run -p vips -i native -n 4",
        "cpus": "0,1,2,3"
    },   
    {
        "name": "canneal",
        "image": "anakli/cca:parsec_canneal",
        "command": "./bin/parsecmgmt -a run -p canneal -i native -n 2",
        "cpus": "2,3"
    },
    {
        "name": "ferret",
        "image": "anakli/cca:parsec_ferret",
        "command": "./bin/parsecmgmt -a run -p ferret -i native -n 2",
        "cpus": "0,1"
    }, 
    {
        "name": "freqmine",
        "image": "anakli/cca:parsec_freqmine",
        "command": "./bin/parsecmgmt -a run -p freqmine -i native -n 2",
        "cpus": "0,1"
    },
    {
        "name": "blackscholes",
        "image": "anakli/cca:parsec_blackscholes",
        "command": "./bin/parsecmgmt -a run -p blackscholes -i native -n 1",
        "cpus": "2"
    },
    {
        "name": "dedup",
        "image": "anakli/cca:parsec_dedup",
        "command": "./bin/parsecmgmt -a run -p dedup -i native -n 1",
        "cpus": "0"
    }
]

def create(job_info, parsec_containers):
    names_of_containers = [cont.name for cont in parsec_containers]
    if(job_info['name'] not in names_of_containers):
        container = client.containers.run(name=job_info['name'],
                                                detach=True,
                                                auto_remove=False,
                                                image=job_info['image'],
                                                command=job_info['command'],
                                                cpuset_cpus=job_info['cpus'])
        logger.job_start(job_info['name'], job_info['cpus'], job_info['command'][-1])
    else : return None
    return container

def pause_container(container):
    container.pause()
    container.reload()
    logger.job_pause(container.name)
 

def unpause_container(container):
    container.unpause()
    container.reload()
    logger.job_unpause(container.name)


def update_cpu_set(container, cpu_set):
    logger.update_cores(container, cpu_set)
    container.update(cpuset_cpus=cpu_set)

def adjust_resources(parsec_containers,pointer2):
    cpu_usage = psutil.cpu_percent(interval=None, percpu=True)
    print(cpu_usage)
    total_usage = sum(cpu_usage)   
    required = parsec_jobs[pointer2]['cpus'].split(',')
    ids = list(map(lambda x: int(x), required))
    min_cpu_usage = min(cpu_usage[id] for id in ids)
    max_cpu_usage = max(cpu_usage[id] for id in ids)
    min_cpu_id = cpu_usage.index(min_cpu_usage)
    max_cpu_id = cpu_usage.index(max_cpu_usage)

    if total_usage > 380:
        if max_cpu_usage >= 95:
            for container in parsec_containers:
                cpus = container.attrs['HostConfig']['CpusetCpus']
                cpus_list = cpus.split(',')
                if str(max_cpu_id) in cpus_list:
                    if container and not container.status == "paused" and container.status == "running" and not container.status == "created" and not container.status == "exited":
                        print(container.status)
                        pause_container(container)

            pointer2 = (pointer2 + 1) % 7
            return None, pointer2
        
    for container in parsec_containers:
        cpus = container.attrs['HostConfig']['CpusetCpus']
        cpus_list = cpus.split(',')
        if str(min_cpu_id) in cpus_list and container.status == "paused":
            unpause_container(container)
    container = create(parsec_jobs[pointer2], parsec_containers)
    pointer2 = (pointer2 + 1) % 7
    return container, pointer2

def start_or_unpause_container(container):
    if container and container.status == "paused":
        unpause_container(container)


def memcached_pid():
    for proc in psutil.process_iter():
        if "memcache" in proc.name():
            return proc.pid

def set_memcached_cpu(pid, cpus):
    command = f'sudo taskset -a -cp {cpus} {pid}'
    logger.update_cores("memcached", cpus)
    subprocess.run(command.split(" "), stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)


def stop_containers(containers):
    for container in containers:
        container.stop()
        container.remove()

def main():
    pointer = 0
    parsec_containers = []
    completed_jobs = []

    try:
        while True:
            container, pointer = adjust_resources(parsec_containers,pointer)

            if container:
                parsec_containers.append(container)
                container.reload()

            for container in parsec_containers:
                container.reload()
                if(container.status == "exited" and container not in completed_jobs):
                    logger.job_end(container.name)
                    completed_jobs.append(container)
                container_id = container.name
                logs = container.logs(tail=300)


                file_name = f"{container_id}.txt"
                with open(file_name, "w") as file:
                    #time.sleep(20)
                    file.write(logs.decode())

           # print(completed_jobs)
           # print(len(completed_jobs))
           # print(len(parsec_jobs))
            
            if len(completed_jobs) == len(parsec_jobs):
                break 

            time.sleep(10)

    finally:
        stop_containers(parsec_containers)

if __name__ == "__main__":
    main()
