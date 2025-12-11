#!/bin/bash

# Make directory for the logs

# Launch the memcached job
echo "Launching the memcached job..."
kubectl create -f ./part3-scheduling-v2/memcache-t1-cpuset.yaml

# Wait for the memcached pod to start running
while [[ $(kubectl get pods -l name=some-memcached -o 'jsonpath={..status.conditions[?(@.type=="Ready")].status}') != "True" ]]; do echo "waiting for memcache pod" && sleep 1; done

# Expose the memcached service
echo "Exposing the memcached service..."

# Check if the service already exists
if kubectl get svc | grep -q some-memcached-11211; then
  # If it does, delete it
  echo "Service some-memcached-11211 already exists, deleting it..."
  kubectl delete svc some-memcached-11211
fi
kubectl expose pod some-memcached --name some-memcached-11211 --type LoadBalancer --port 11211 --protocol TCP

# Wait for the service to be ready
echo "Waiting for the memcached service to be ready..."
sleep 60

# Get the IP of the memcached pod
MEMCACHED_IP=$(kubectl get pod some-memcached -o jsonpath="{.status.podIP}")

# SSH into the client-agent-a and client-agent-b machines and start the mcperf load
echo "Starting mcperf load on client-agent-a and client-agent-b..."
gcloud compute ssh --ssh-key-file ~/.ssh/cloud-computing ubuntu@client-agent-a-5qlp --zone europe-west3-a --command "./memcache-perf-dynamic/mcperf -T 2 -A &" &
gcloud compute ssh --ssh-key-file ~/.ssh/cloud-computing ubuntu@client-agent-b-6q6f --zone europe-west3-a --command "./memcache-perf-dynamic/mcperf -T 4 -A &" &

# Get the internal IP addresses of the client-agent-a and client-agent-b
INTERNAL_AGENT_A_IP=$(kubectl get nodes client-agent-a-5qlp -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}')
INTERNAL_AGENT_B_IP=$(kubectl get nodes client-agent-b-6q6f -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}')

# SSH into the client-measure machine and start the mcperf load
gcloud compute ssh --ssh-key-file ~/.ssh/cloud-computing ubuntu@client-measure-gwdb --zone europe-west3-a --command "./memcache-perf-dynamic/mcperf -s $MEMCACHED_IP --loadonly; ./memcache-perf-dynamic/mcperf -s $MEMCACHED_IP -a $INTERNAL_AGENT_A_IP -a $INTERNAL_AGENT_B_IP --noload -T 6 -C 4 -D 4 -Q 1000 -c 4 -t 10 --scan 30000:30500:5" >> part3-scheduling-v2/mcperf_output.log 2>&1 &

# Wait for the mcperf load to start
echo "Waiting for the mcperf load to start..."
sleep 120

# Launch the PARSEC jobs
echo "Launching PARSEC jobs..."
for job in $(ls part3-scheduling-v2/parsecs); do
    echo "Launching $job..."
    kubectl create -f part3-scheduling-v2/parsecs/$job
done

# Wait for all jobs to finish
echo "Waiting for all jobs to finish..."

# Count the number of jobs
num_jobs=$(ls part3-scheduling-v2/parsecs | wc -l)

# Wait for all jobs to finish
while true; do
    num_completed=$(kubectl get jobs | grep '1/1' | wc -l)
    if [ "$num_completed" -eq "$num_jobs" ]; then
        break
    fi
    echo "waiting for jobs" && sleep 1
done

# Wait for the mcperf load to finish
echo "Waiting for the mcperf load to finish..."
sleep 30

# Once all jobs have completed, get the pod info
echo "Fetching and saving logs for each job..."
kubectl get pods -o json > results.json

# Fetch and save logs for each job
for job in $(ls part3-scheduling-v2/parsecs); do
    job_name=$(basename $job .yaml)
    log_file="logs/${job_name}.log"
    pod_name=$(kubectl get pods --selector=job-name=${job_name} --output=jsonpath='{.items[*].metadata.name}')
    kubectl logs $pod_name > $log_file
done

# Parse the JSON output to get the execution time of the batch jobs
python3 get_time.py results.json

echo "Kill the mcperf load..."
# SSH into the client-agent-a and client-agent-b machines and kill the mcperf load
gcloud compute ssh --ssh-key-file ~/.ssh/cloud-computing ubuntu@client-agent-a-5qlp --zone europe-west3-a --command "pkill mcperf" &
gcloud compute ssh --ssh-key-file ~/.ssh/cloud-computing ubuntu@client-agent-b-6q6f --zone europe-west3-a --command "pkill mcperf" &

# SSH into the client-measure machine and kill the mcperf load
gcloud compute ssh --ssh-key-file ~/.ssh/cloud-computing ubuntu@client-measure-gwdb --zone europe-west3-a --command "pkill mcperf" &

echo "Done!"