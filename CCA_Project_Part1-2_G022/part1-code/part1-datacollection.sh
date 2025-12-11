# Parameter 1: MEMCACHED_IP
# Parameter 2: INTERNAL_AGENT_IP
# Parameter 3: output filename

echo "type,avg,std,min,p5,p10,p50,p67,p75,p80,p85,p90,p95,p99,p999,p9999,QPS,target" > ~/$3.csv
for i in {1..3}
do
  echo "Running your command for the $i time..."
  start_time=$(date +%s.%N) # get the start time in seconds
  ./mcperf -s $1 -a $2 --noload -T 16 -C 4 -D 4 -Q 1000 -c 4 -w 2 -t 5 --scan 30000:110000:5000 | awk 'NR != 1 && NR != 19 && NR != 20' | sed 's/ \+/,/g' >> ~/$3.csv # exclude rows using awk
  end_time=$(date +%s.%N) # get the end time in seconds
  elapsed=$(echo "$end_time - $start_time" | bc) # calculate the elapsed time in seconds
  echo "Done. The command took $elapsed seconds."
done