import docker
import psutil
import subprocess
import time

from datetime import datetime
from enum import Enum
import urllib.parse

LOG_STRING = "{timestamp} {event} {job_name} {args}"

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

class ParsecJob:
    def __init__(self, name: Job, image: str, command: str, cpus: str, threads: int):
        self.name = name
        self.image = image
        self.command = command
        self.cpus = cpus
        self.threads = threads

# Define your Parsec jobs
long_jobs = [
    ParsecJob(Job.CANNEAL, "anakli/cca:parsec_canneal", "./bin/parsecmgmt -a run -p canneal -i native", "2,3", 4),
    ParsecJob(Job.FERRET, "anakli/cca:parsec_ferret", "./bin/parsecmgmt -a run -p ferret -i native", "2,3", 2),
    ParsecJob(Job.FREQMINE, "anakli/cca:parsec_freqmine", "./bin/parsecmgmt -a run -p freqmine -i native", "2,3", 2),
    ParsecJob(Job.BLACKSCHOLES, "anakli/cca:parsec_blackscholes", "./bin/parsecmgmt -a run -p blackscholes -i native", "2,3", 4)
]

short_jobs = [
    ParsecJob(Job.VIPS, "anakli/cca:parsec_vips", "./bin/parsecmgmt -a run -p vips -i native", "1", 4),
    ParsecJob(Job.RADIX, "anakli/cca:splash2x_radix", "./bin/parsecmgmt -a run -p splash2x.radix -i native", "1", 4),
    ParsecJob(Job.DEDUP, "anakli/cca:parsec_dedup", "./bin/parsecmgmt -a run -p dedup -i native", "1", 4)
]

def main():
    logger = SchedulerLogger()
    client = docker.from_env()
    long_jobs_queue = list(long_jobs)
    short_jobs_queue = list(short_jobs)
    running_jobs = []
    completed_jobs = []
    memcached_on_core1 = False

    def start_job(job_info):
        container = client.containers.run(name=job_info.name.value, detach=True, image=job_info.image,
                                        command=f"{job_info.command} -n {job_info.threads}", cpuset_cpus=job_info.cpus)
        container.cpus = job_info.cpus  # Attach the cpuset_cpus as a new attribute to the container object
        logger.job_start(job_info.name, job_info.cpus.split(','), job_info.threads)
        return container

    def stop_job(job_container):
        job_container.stop()
        job_container.reload()
        logger.job_end(Job[job_container.name.upper()])

    def pause_job(job_container):
        job_container.pause()
        job_container.reload()
        logger.job_pause(Job[job_container.name.upper()])

    def unpause_job(job_container):
        job_container.unpause()
        job_container.reload()
        logger.job_unpause(Job[job_container.name.upper()])

    def updates_cores(mem_pid, cores):
        set_memcached_cpu(mem_pid, cores)
        logger.update_cores("memcached", cores.split(','))

    def write_logs(job_container):
        logs = job_container.logs(tail=300)
        with open(f"{job_container.name}.txt", "w") as file:
            file.write(logs.decode())

    memcached_pid = get_process_pid('memcached')
    set_memcached_cpu(memcached_pid, '0')
    time.sleep(20)
    
    try:
        while True:
            cpu_usage = psutil.cpu_percent(interval=None, percpu=True)
            print(cpu_usage)
            # If core 0 usage > 40% and memcached is not already running core 1 aswell, we set memcached to run on both cores
            if cpu_usage[0] > 40 and not memcached_on_core1:
                updates_cores(memcached_pid, '0,1')
                memcached_on_core1 = True
                # Pause all jobs running on core 1
                for job_container in running_jobs:
                    print(job_container.cpus == '1' and job_container.status != 'paused' and job_container.status == 'running')
                    if job_container.cpus == '1' and job_container.status != 'paused' and job_container.status == 'running':
                        pause_job(job_container)
                        print("Pause")

            # If core 0 usage + core 1 usage < 50% and memcached is running on core 1, we set memcached to run only on core 0
            if cpu_usage[0] + cpu_usage[1] < 35 and memcached_on_core1:
                updates_cores(memcached_pid, '0')
                memcached_on_core1 = False
                # Unpause all jobs running on core 1
                for job_container in running_jobs:

                    if job_container.cpus == '1' and job_container.status == 'paused':
                        unpause_job(job_container)
                        print("Unpause")

            # This writes the logs and stops the containers that have finished
            for job_container in running_jobs:
                job_container.reload()
                write_logs(job_container)

                if job_container.status == 'exited':
                    stop_job(job_container)
                    completed_jobs.append(job_container)
                    running_jobs.remove(job_container)  # remove the job from running_jobs list

            
            # This checks if all jobs have finished
            if not any(job.status != 'exited' for job in running_jobs) and not long_jobs_queue and not short_jobs_queue:
                print("Finish check true")
                print("Long job queue:")
                for job in long_jobs_queue:
                    print(job.name)
                print("Short job queue:")
                for job in short_jobs_queue:
                    print(job.name)
                break
            
            # Check if long job is running
            if not any(job.cpus  == "2,3" for job in running_jobs) and long_jobs_queue:
                running_jobs.append(start_job(long_jobs_queue.pop(0)))
            elif not any(job.cpus  == "1" for job in running_jobs) and short_jobs_queue:
                running_jobs.append(start_job(short_jobs_queue.pop(0)))

            # If there are no more long jobs and core 2,3 is free, start a short job there.
            if not long_jobs_queue and not any(job.cpus == "2,3" for job in running_jobs) and short_jobs_queue:
                job_to_start = short_jobs_queue.pop(0)
                job_to_start.cpus = "2,3"  
                running_jobs.append(start_job(job_to_start))

            time.sleep(6)


    finally:
        for job in running_jobs:
            if job.status != 'exited':
                stop_job(job)
        logger.end()

def get_process_pid(process_name):
    for proc in psutil.process_iter(['name']):
        if proc.info['name'] == process_name:
            return proc.pid
    return None

def set_memcached_cpu(pid, cpus):
    subprocess.run(['taskset', '-pc', cpus, str(pid)])
    
    

if __name__ == '__main__':
    main()