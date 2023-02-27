import json
import subprocess
import os
import time

usr_name = "hexiang"
TestBatch = 5
Local = False
main_command = ""
# docker run -it --name=fc-node --network=host fc ./bin/fc-server -node=c -addr=10.10.10.30:2001
# docker run -it --name=fc-node --network=host fc ./bin/fc-server -node=p -addr=10.10.10.31:2001
# docker run -dt --name=fc-learner --network=host learner sh
# ssh -@10.10.10.31 docker rm -f fc-node
# docker run -dt --name=fc-node --network=host fc
run_participant = "docker exec -dt fc-node ./bin/fc-server -node=p "
run_participant_local = ["./bin/fc-server -node=p -local " for _ in range(3)]

run_rl_server_cmd = "docker exec -it fc-learner /venv/bin/python /app/main.py "
run_clear = "docker restart fc-node"
run_clear_local = ["docker restart fc-node%d" % (i+1) for i in range(3)]
run_clear_rl = "docker restart fc-learner"
protocols = ["fc", "3pc", "2pc", "fcff", "fccf", "easy", "pac", "gpac"]
logf = open("./logs/progress.log", "w+")

if Local:
    run_client_cmd = "./bin/fc-server -local -node=c"
    with open("./configs/local.json") as f:
        config = json.load(f)
else:
    run_client_cmd = "docker exec -it fc-node ./bin/fc-server -node=c "
    with open("./configs/remote.json") as f:
        config = json.load(f)

for id_ in config["coordinators"]:
    run_client_cmd = run_client_cmd + " -addr=" + config["coordinators"][id_]


def get_server_cmd(addr, r, ml, cf, nf, delay_var, np, store, bench, wh, dis):
    if Local:
        return run_participant_local[int(addr[-1]) - 1] + " -addr=" + str(addr) + \
              " -r=" + str(r) + \
              " -cf=" + str(cf) + \
              " -nf=" + str(nf) + \
              " -dvar=" + str(delay_var) + \
              " -part=" + str(np) + \
              " -bench=" + str(bench) + \
              " -wh=" + str(wh) + \
              " -store=" + str(store) + \
              " -dis=" + str(dis) + \
              " -ml=" + str(ml)
    else:
        return run_participant + " -addr=" + str(addr) + \
              " -r=" + str(r) + \
              " -cf=" + str(cf) + \
              " -nf=" + str(nf) + \
              " -dvar=" + str(delay_var) + \
              " -part=" + str(np) + \
              " -bench=" + str(bench) + \
              " -wh=" + str(wh) + \
              " -store=" + str(store) + \
              " -dis=" + str(dis) + \
              " -ml=" + str(ml)


def get_client_cmd(bench, protocol, clients, file, cf=-1, nf=-1, skew=0.5, cross=20, length=16, txn_part=2, np=3, rw=0.5
                   , tb=10000, r=2.0, dis="exp", elapsed=False, replica=False, delay_var=0.0,
                   store="benchmark", debug="false", wh=1, optimizer="rl"):
    return run_client_cmd + " -bench=" + str(bench) + \
        " -p=" + str(protocol) + \
        " -c=" + str(clients) + \
        " -nf=" + str(nf) + \
        " -cf=" + str(cf) + \
        " -wh=" + str(wh) + \
        " -debug=" + str(debug) + \
        " -skew=" + str(skew) + \
        " -cross=" + str(cross) + \
        " -len=" + str(length) + \
        " -part=" + str(np) + \
        " -rw=" + str(rw) + \
        " -r=" + str(r) + \
        " -dis=" + str(dis) + \
        " -txn_part=" + str(txn_part) + \
        " -elapsed=" + str(elapsed) + \
        " -store=" + str(store) + \
        " -replica=" + str(replica) + \
        " -dvar=" + str(delay_var) + \
        " -optimizer=" + str(optimizer) + \
        " -tb=" + str(tb) + file


# gcloud beta compute ssh --zone "asia-southeast1-a" "cohort1" -- '
def execute_cmd_in_gcloud(zone, instance, cmd):
    cmd = "gcloud beta compute ssh --zone " + "%s %s -- \'" % (zone, instance) + " " + cmd + "\'"
    ssh = subprocess.Popen(cmd,
                           shell=True,
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
    return ssh


def call_cmd_in_ssh(address, cmd):
    cmd = "ssh " + usr_name + "@" + address + " " + cmd + ">./server.log"
    print(cmd)
    ssh = subprocess.call(cmd,
                           shell=True,
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)


def execute_cmd_in_ssh(address, cmd):
    cmd = "ssh " + usr_name + "@" + address + " " + cmd + ">./server.log"
    print(cmd)
    ssh = subprocess.Popen(cmd,
                           shell=True,
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
    return ssh


def run_task(cmd, print_log=True):
    if print_log:
        print(cmd)

    print(cmd, file=logf)
    logf.flush()
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                         shell=True, preexec_fn=os.setsid)
    return p


def start_participant(service, r, minlevel, cf, nf, delay_var, np, store, bench, wh, dis):
    cmd = get_server_cmd(service, r, minlevel, cf, nf, delay_var, np, store, bench, wh, dis)
    if Local:
        return
        # return run_task(cmd)
    else:
        return call_cmd_in_ssh(service.split(':')[0], cmd)


#    return execute_cmd_in_gcloud(zone, instance, cmd)

def start_service_on_all(r=2.0, run_rl=True, rl_time=10, ml=1, cf=-1, nf=-1, num=3,
                         delay_var=0.0, store="benchmark", bench="ycsb", wh=1, dis="normal"):
    if run_rl:
        run_task(run_rl_server_cmd + ">./train.log", print_log=False)
    if Local:
        return
    for i in config["participants"]:
        if i == "1":  # the crash failure is only injected to node 1.
            start_participant(config["participants"][i], r, ml, cf, nf, delay_var, num, store, bench, wh, dis)
        elif int(i) <= num:
            if dis == "normal":
                start_participant(config["participants"][i], r, ml, -1, -1, delay_var, num, store, bench, wh, dis)
            else:
                start_participant(config["participants"][i], r, ml, 0, -1, delay_var, num, store, bench, wh, dis)


def terminate_service(num=3, run_rl=False):
    if run_rl:
        p = run_task(run_clear_rl, print_log=False)
        p.wait()

    if Local:
        return
    else:
        for i in config["participants"]:
            if int(i) <= num:
                p = execute_cmd_in_ssh(config["participants"][i].split(':')[0], run_clear)
                p.wait()


def delete_extra_zero(n):
    if isinstance(n, int):
        return str(n)
    if isinstance(n, float):
        n = str(n).rstrip('0')
        if n.endswith('.'):
            n = n.rstrip('.')
        return n
    return "Noooooooo!!!!"


def run_client_number_failure_free(bench):
    l = [1, 16, 64, 128, 256, 512]
    filename = ">./results/ycsb_ff.log"
    for c in l:
        rnd = TestBatch
        for po, lv in [("fc", -1), ("2pc", -1), ("easy", -1), ("3pc", -1), ("pac", -1)]:  # fixed level
            for each in range(rnd):
                start_service_on_all()
                p = run_task(get_client_cmd(bench, po, c, filename))
                p.wait()
                terminate_service()
                if filename[1] == '.':
                    filename = ">" + filename


def run_replicated():
    l = [1, 16, 64, 128, 256, 512]
    filename = ">./results/replicated.log"
    for c in l:
        rnd = TestBatch
        for po, lv in [("gpac", -1)]:  # fixed level
            for each in range(rnd):
                start_service_on_all()
                p = run_task(get_client_cmd("ycsb", po, c, filename))
                p.wait()
                terminate_service()
                if filename[1] == '.':
                    filename = ">" + filename


def run_breakdown():
    l = [16, 512]
    filename = ">./results/breakdown.log"
    for c in l:
        rnd = TestBatch
        for po, lv in [("fc", -1), ("2pc", -1), ("easy", -1), ("3pc", -1), ("pac", -1)]:  # fixed level
            for each in range(rnd):
                start_service_on_all()
                p = run_task(get_client_cmd("ycsb", po, c, filename))
                p.wait()
                terminate_service()
                if filename[1] == '.':
                    filename = ">" + filename


def run_crash_failure():
    c = 512
    rnd = TestBatch
    filename = ">./results/crash_failure.log"
    for cf_period in [-1, 1000, 100, 10, 0]:
        for po, lv in [("fc", -1), ("2pc", -1), ("easy", -1), ("3pc", -1), ("pac", -1)]:  # fixed level
            for each in range(rnd):
                start_service_on_all(cf=cf_period)
                p = run_task(get_client_cmd("ycsb", po, c, filename, cf=cf_period))
                p.wait()
                terminate_service()
                if filename[1] == '.':
                    filename = ">" + filename


def run_network_var():
    c = 512
    rnd = TestBatch
    filename = ">./results/network_var.log"
    for vari in [0.25 * i for i in range(7)]:
        for r in [1.5]:#[0.5, 1, 2, 4, 8, 16]:
            for po, _ in [("fc", -1)]:  # fixed level
                for each in range(rnd):
                    start_service_on_all(delay_var=vari, nf=0, r=r)
                    p = run_task(get_client_cmd("ycsb", po, c, filename, delay_var=vari, nf=0, r=r))
                    p.wait()
                    terminate_service()
                    if filename[1] == '.':
                        filename = ">" + filename


def run_real_world_db():
    rnd = TestBatch
    filename = ">./results/real_db.log"
    for store in ["mongo", "sql"]:
        for po, _ in [("fc", -1), ("2pc", -1), ("easy", -1), ("3pc", -1), ("pac", -1)]:
            for each in range(rnd):
                start_service_on_all(store=store)
                time.sleep(10)
                p = run_task(get_client_cmd("ycsb", po, 512, filename, store=store))
                p.wait()
                terminate_service()
                if filename[1] == '.':
                    filename = ">" + filename
    store = 'sql'
    for po, _ in [("fc", -1), ("2pc", -1), ("easy", -1), ("3pc", -1), ("pac", -1)]:
        for each in range(rnd):
            start_service_on_all(store=store)
            time.sleep(10)
            p = run_task(get_client_cmd("ycsb", po, 128, filename, store=store, skew=0.8, length=4))
            p.wait()
            terminate_service()
            if filename[1] == '.':
                filename = ">" + filename
    store = 'mongo'
    for po, _ in [("fc", -1), ("2pc", -1), ("easy", -1), ("3pc", -1), ("pac", -1)]:
        for each in range(rnd):
            start_service_on_all(store=store)
            time.sleep(10)
            p = run_task(get_client_cmd("ycsb", po, 512, filename, store=store, skew=0.8, length=4))
            p.wait()
            terminate_service()
            if filename[1] == '.':
                filename = ">" + filename
    for po, _ in [("fc", -1), ("2pc", -1), ("easy", -1), ("3pc", -1), ("pac", -1)]:
        for each in range(rnd):
            start_service_on_all(store="sql")
            time.sleep(10)  # wait for inserts to finish.
            p = run_task(get_client_cmd("ycsb", po, 128, filename, store="sql", length=4, skew=0.8))
            p.wait()
            terminate_service()
            if filename[1] == '.':
                filename = ">" + filename
    for po, _ in [("fc", -1), ("2pc", -1), ("easy", -1), ("3pc", -1), ("pac", -1)]:
        for each in range(rnd):
            start_service_on_all(store="mongo")
            time.sleep(10)
            p = run_task(get_client_cmd("ycsb", po, 512, filename, store="mongo"))
            # , skew=0.8
            p.wait()
            terminate_service()
            if filename[1] == '.':
                filename = ">" + filename


def run_skew():
    c = 512
    r = 2
    rnd = TestBatch
    filename = ">./results/ycsb_skew.log"
    for sk in [0.0, 0.2, 0.4, 0.6, 0.8]:
        for po, lv in [("fcff", -1), ("2pc", -1), ("easy", -1), ("3pc", -1), ("pac", -1)]:  # fixed level
            for each in range(rnd):
                start_service_on_all(r)
                p = run_task(get_client_cmd("ycsb", po, c, filename, skew=sk))
                p.wait()
                terminate_service()
                if filename[1] == '.':
                    filename = ">" + filename


def run_tpcc():
    c = 2000
    r = 2
    rnd = TestBatch
    filename = ">./results/tpcc_wh.log"
    for wh in [1, 4, 16, 64, 128]:
        for po, lv in [("fc", -1), ("2pc", -1), ("easy", -1), ("3pc", -1), ("pac", -1)]:  # fixed level
            for each in range(rnd):
                start_service_on_all(r, bench="tpc", wh=wh)
                time.sleep(5)
                p = run_task(get_client_cmd("tpc", po, c, filename, wh=wh))
                p.wait()
                terminate_service()
                if filename[1] == '.':
                    filename = ">" + filename


def run_sensitive():
    c = 512
    rnd = TestBatch
    filename = ">./results/sensitive.log"
    for cross in [0, 20, 40, 50, 60, 80, 100]:
        for po, lv in [("fc", -1), ("2pc", -1), ("easy", -1), ("3pc", -1), ("pac", -1)]:  # fixed level
            for each in range(rnd):
                start_service_on_all()
                p = run_task(get_client_cmd("ycsb", po, c, filename, cross=cross))
                p.wait()
                terminate_service()
                if filename[1] == '.':
                    filename = ">" + filename


def run_scale():
    c = 512
    rnd = TestBatch
    filename = ">./results/scale.log"
    for num in [2, 4, 6, 8, 10]:
        r = max(2.0, num/2.0+1.0)
        for po, lv in [("fc", -1), ("2pc", -1), ("easy", -1), ("3pc", -1), ("pac", -1)]:  # fixed level
            for each in range(rnd):
                start_service_on_all(num=num, r=r)
                p = run_task(get_client_cmd("ycsb", po, c, filename, np=num, r=r))
                p.wait()
                terminate_service(num=num)
                if filename[1] == '.':
                    filename = ">" + filename


def run_participant_num():
    c = 512
    rnd = TestBatch
    filename = ">./results/part_num.log"
    np = 8
    for num in [1, 2, 3, 4, 6, 8]:
        r = max(2.0, np/2.0+1.0)
        for po, lv in [("fc", -1), ("2pc", -1), ("easy", -1), ("3pc", -1), ("pac", -1)]:  # fixed level
            for each in range(rnd):
                start_service_on_all(num=np, r=r)
                p = run_task(get_client_cmd("ycsb", po, c, filename, txn_part=num, np=np, r=r))
                p.wait()
                terminate_service(num=np)
                if filename[1] == '.':
                    filename = ">" + filename


def test_conn():
    filename = ">./test.log"
    for po, _ in [("2pc", -1)]:
        start_service_on_all()
        time.sleep(5)
        p = run_task(get_client_cmd("ycsb", po, 512, filename))
        p.wait()
        terminate_service()
        if filename[1] == '.':
            filename = ">" + filename


def run_ablation():
    c = 512
    rnd = TestBatch
    filename = ">./results/ablation.log"
    # for cf_period, nf_period in [(-1, -1), (0, -1), (-1, 0)]:
    for cf_period, nf_period in [(-1, 0)]: # , ("fccf", -1), ("fcff", -1), ("easy", -1)
        for po, lv in [("fc", -1), ("fccf", -1), ("fcff", -1), ("easy", -1)]:  # fixed level
            for each in range(rnd):
                start_service_on_all(cf=cf_period, nf=nf_period, delay_var=10)
                p = run_task(get_client_cmd("ycsb", po, c, filename))
                p.wait()
                terminate_service()
                if filename[1] == '.':
                    filename = ">" + filename


def run_trace():
    c = 512
    rnd = TestBatch
    filename = ">./results/trace.log"
    for po in ["fc", "2pc"]:
        for trace in ["instagram"]:  # fixed level
        # for trace in ["apple_server", "facebook", "github",
        #                  "gmail", "instagram", "netflix", "twitter", "youtube", "whatsapp", "skype"]:  # fixed level
            for each in range(rnd):
                start_service_on_all(cf=0, dis=trace)
                p = run_task(get_client_cmd("ycsb", po, c, filename, cf=0, dis=trace, elapsed=True))
                p.wait()
                terminate_service()
                if filename[1] == '.':
                    filename = ">" + filename


if __name__ == '__main__':
    test_conn()
    # run_replicated()
    # run_trace()
# failure-free environment.
# failure-free experiments to show FC's performance.
#     run_client_number_failure_free("ycsb")  # --> Figure 1 (a) (b) client number exp (e) (f) latency breakdown
#     run_skew()      # --> Figure 1 (c) (d) skewness impact.
#     run_breakdown()
#     run_tpcc()
# cloud experiments (Figure 7)
    # a TPCC experiment + a ycsb experiment.
    # run_client_number_failure_free("tpc")

# scalability experiments (Figure 2).
#     run_scale()             # (a) --> a experiment to show scalability of FC, linear up.
#     run_participant_num()   # (b) --> a experiment to show scalability of FC, get down.

# network variability experiments (Figure 3a).
    # x-axis: the network variety, y-axis: the parameter r. Default setting, show the experiment results.
    # run_network_var()
    # run_real_world_db()
# crash failure (Figure 3b)
#     run_crash_failure()     # Figure 4 --> Figure a new experiment to show the impact of fcCF.
    # run_network_failure()

# ablation study (Figure 5): three different figures: x-axis is the expected time for failures to reoccur,
# y-axis is the throughput.
    # the full FC
    # the FC w/o FC-FF --> bad in most cases.
    # the FC w/o FC-CF --> bad when the crash failure happens.
    # the FC w/o the network parameter tuning (RL-based). --> bad when the crash failure happens in high frequency.
    # run_ablation()

# how to showcase the effect of RL-tuner? (Figure 6)
    # the FC w/o the network parameter tuning (RL-based). the parameter is fixed to 1, 10, 100, 1000 or RL-tuned
    # poisson distribution.
    # normal distribution.
    # plain distribution.
    # run_rl()

# experiments to be added to extended version.
# run_sensitive() --> experiments to be added to arxiv extended version.
# run_replica() --> experiments to be added to arxiv extended version.
# run_tpc() --> experiments to be added to arxiv extended version.
# run_side() --> negative effect of the message complexity.
# run_delay() --> the effect of the network delay.

#    logf.close()
