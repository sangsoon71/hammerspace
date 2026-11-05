This code can be used to do file benchmark test on multiple clients using fio

Pre-requisite:
1. install fio on clients
2. run fio in server mode (fio --server)
3. open required port 8765 if needed

How to run:
python3 nfs_fio_bench.py --mount <testing_path> --clients <clientname or ip address>, --sheet-id <google sheet id> --creds <credential for accessing google sheet> --run <no. of run> --bs <block_size> --iodepth <iodepth> --numjubs <no. of jobs> --size <file_size> --runtime <runtime in seconds> --ramp_time <ramp_time in seconds>

Example:
python3 nfs_fio_bench.py --mount /mnt/hammer/tier0 --clients client1,client2 --sheet-id 1_EKPRHnh90s9lzCRRP1kv8xOjXtGgWcsuBCmcDjl0Qw --creds gen-lang-client-0266048254-890dddd8c9af.json --run
s 3 --bs 4k,16k,32k,64k,1m,10m --iodepth 16 --numjobs 16 --size 8g --runtime 120 --ramp-time 60 --startdelay 10 --refill-buffers --norandommap --random-generator tausworthe64 --nrfiles 16
