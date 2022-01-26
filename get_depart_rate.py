from cmath import log
import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt


def get_statistic_df(log_folder='/home/labuser/Downloads/MQSim/logs/short_lat_test'):
    # multi-level column can be created from tuples, regardless of what the original column names, e.g., ['a', 'b', 'c', 'd', 'e']
    ret_df = pd.DataFrame(columns=['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i'])
    ret_df.columns=pd.MultiIndex.from_tuples([("test", ""), ("read", "throughput"), ("read", "ave_lat"), ("read", "last_lat"), ("read", "90th_lat"), ("write", "throughput"), ("write", "ave_lat"), ("write", "last_lat"), ("write", "90th_lat")])


    for root, dirs, files in os.walk(log_folder):
        for f in files:
            if ".csv" in f:
                path = os.path.join(root, f)
                df = pd.read_csv(path)
                r_df=df[df.IOType==0]
                w_df=df[df.IOType==1]
                last_r_idx = r_df.FinishTime.idxmax()
                last_w_idx = w_df.FinishTime.idxmax()
                # print(f, "read finish rate: {}, write finish rate: {}".format(np.diff(r_df.FinishTime.values).mean()/1e3, np.diff(w_df.FinishTime.values).mean()/1e3))
                #print(f, "read tpt: {} GB/s, write tpt: {} GB/s".format(r_df.Size.sum()/(r_df.FinishTime.iloc[len(r_df)-1]), w_df.Size.sum()/(w_df.FinishTime.iloc[len(w_df)-1]) ))
                ret_df.loc[len(ret_df), :] = [f, r_df.Size.sum()/(r_df.FinishTime.max() - r_df.ArrivalTime.min()), r_df.DelayTime.mean()/1e3, r_df.loc[last_r_idx, "DelayTime" ]/1e3, r_df.DelayTime.quantile(0.9)/1e3 \
                                    ,w_df.Size.sum()/(w_df.FinishTime.max()-w_df.ArrivalTime.min()), w_df.DelayTime.mean()/1e3, w_df.loc[last_w_idx, "DelayTime" ]/1e3,  w_df.DelayTime.quantile(0.9)/1e3] 

    ret_df = ret_df.sort_values(by=["test"], ignore_index=True)
    print(ret_df)
    return ret_df

def plot_runtime_throughput(log_folder='/home/labuser/Downloads/MQSim/logs/short_lat_test', bucket_size=1e6):
    figure, axs = plt.subplots(2)
    trace_files = []
    trace_dirt = {}
    for root, dirs, files in os.walk(log_folder):
        for f in files:
            if ".csv" in f:
                trace_files.append(os.path.join(root, f))

    trace_files.sort(key=lambda x: eval(x.split("_")[-2]))
    for f in trace_files:
        df = pd.read_csv(f)
        df = df.sort_values(by="FinishTime", ignore_index=True)
        df["runtime_throughput"] = df.Size/df.DelayTime * 8 # Gbps 
        df["time_ms"] = (df.FinishTime/bucket_size).astype(int) + 1 # bucket with size of 1 ms
        r_df=df[df.IOType==0]
        w_df=df[df.IOType==1]
        r_tpt = r_df.groupby(["time_ms"]).sum()
        w_tpt = w_df.groupby(["time_ms"]).sum()
        trace_dirt[f] = [r_tpt.runtime_throughput, w_tpt.runtime_throughput]
        marker = ''
        linestyle = "solid"
        # if '1_to_1' in f:
        #     marker = 'x'
        #     linestyle = "solid"
        # else:
        #     if '_to_1' in f and (not '1_to_1' in f):
        #         linestyle = "dashed"

        axs[0].plot(r_tpt.runtime_throughput, label=os.path.basename(f).split(".")[0], marker=marker, linestyle=linestyle)
        axs[1].plot(w_tpt.runtime_throughput, label=os.path.basename(f).split(".")[0], marker=marker, linestyle=linestyle)

    axs[0].set_ylabel("throughput (Gbps)")
    axs[0].set_title("read")
    axs[0].set_yscale("log")
    
    axs[1].set_ylabel("throughput (Gbps)")
    axs[1].set_title("write")
    axs[1].set_xlabel("time bin with size {} ns".format(int(bucket_size)))
    axs[1].set_yscale("log")

    figure.set_size_inches(12, 8)
    plt.legend()
    plt.savefig(os.path.join(log_folder, "tpt.png"))
    return trace_dirt

def plot_runtime_arrival_rate(log_folder, bucket_size=1e6):
    figure, axs = plt.subplots(2)
    trace_files = []
    for root, dirs, files in os.walk(log_folder):
        for f in files:
            if ".csv" in f:
                trace_files.append(os.path.join(root, f))

    trace_files.sort()
    for f in trace_files:
        df = pd.read_csv(f)
        df = df.sort_values(by="ArrivalTime", ignore_index=True)
        df["time_ms"] = (df.ArrivalTime/bucket_size).astype(int) + 1 # bucket with size of 1 ms
        r_df=df[df.IOType==0]
        w_df=df[df.IOType==1]
        r_rate = r_df.groupby(["time_ms"]).count()
        w_rate = w_df.groupby(["time_ms"]).count()
        marker = ''
        linestyle = "solid"
        # if '1_to_1' in f:
        #     marker = 'x'
        #     linestyle = "solid"
        # else:
        #     if '_to_1' in f and (not '1_to_1' in f):
        #         linestyle = "dashed"

        axs[0].plot(r_rate.ArrivalTime, label=os.path.basename(f).split(".")[0], marker=marker, linestyle=linestyle)
        axs[1].plot(w_rate.ArrivalTime, label=os.path.basename(f).split(".")[0], marker=marker, linestyle=linestyle)
        break
    axs[0].set_ylabel("Arrival rate")
    axs[0].set_title("read")
    
    axs[1].set_ylabel("Arrival rate")
    axs[1].set_title("write")
    axs[1].set_xlabel("time bin with size {} ns".format(int(bucket_size)))
    # axs[1].set_yscale("log")
    figure.set_size_inches(12, 8)
    plt.legend()
    plt.savefig(os.path.join(log_folder, "rate.png"))

def plot_throughput_summary(log_folder='/home/labuser/Downloads/MQSim/logs/short_lat_test', bucket_size=1e6, quantile=0.5):
    figure, axs = plt.subplots(2)
    trace_files = []
    tpt_df = pd.DataFrame(columns=['f', 'r', 'w'])
    for root, dirs, files in os.walk(log_folder):
        for f in files:
            if ".csv" in f:
                trace_files.append(os.path.join(root, f))

    trace_files.sort(key=lambda x: eval(x.split("_")[-2]))
    for f in trace_files:

        df = pd.read_csv(f)
        df = df.sort_values(by="FinishTime", ignore_index=True)
        df["runtime_throughput"] = df.Size/df.DelayTime * 8 # Gbps 
        df["time_ms"] = (df.FinishTime/bucket_size).astype(int) + 1 # bucket with size of 1 ms
        r_df=df[df.IOType==0]
        w_df=df[df.IOType==1]
        r_tpt = r_df.groupby(["time_ms"]).sum()
        w_tpt = w_df.groupby(["time_ms"]).sum()
        r = r_tpt.runtime_throughput.quantile(quantile)
        w = w_tpt.runtime_throughput.quantile(quantile)
        tpt_df.loc[len(tpt_df), :] = [os.path.basename(f)[4:-12], r, w]
        marker = '*'
        linestyle = "solid"
        # if '1_to_1' in f:
        #     marker = 'x'
        #     linestyle = "solid"
        # else:
        #     if '_to_1' in f and (not '1_to_1' in f):
        #         linestyle = "dashed"

    axs[0].bar(tpt_df.f, tpt_df.r, label=os.path.basename(f).split(".")[0],)
    axs[1].bar(tpt_df.f, tpt_df.w, label=os.path.basename(f).split(".")[0], )

    axs[0].set_ylabel("throughput (Gbps)")
    axs[0].set_title("read")
    # axs[0].set_yscale("log")
    
    axs[1].set_ylabel("throughput (Gbps)")
    axs[1].set_title("write")
    axs[1].set_xlabel("time bin with size {} ns".format(int(bucket_size)))
    # axs[1].set_yscale("log")

    figure.set_size_inches(12, 8)
    plt.legend()
    plt.savefig(os.path.join(log_folder, "tpt_summary.png"))


def plot_runtime_onservice_rate(log_folder, bucket_size=1e6):
    trace_files = []
    for root, dirs, files in os.walk(log_folder):
        for f in files:
            if "scheduler" in f:
                trace_files.append(os.path.join(root, f))
    figure, axs = plt.subplots(3)
    trace_files.sort()
    for f in trace_files:
        with open(f, "r") as scheduler_trace:
            lines = scheduler_trace.read().splitlines()
        trace_lines = [l.split(",") for l in lines if len(l.split(","))==3]
        trace_df = pd.DataFrame(trace_lines, columns=['IOType','idx', 'onservicetime'])
        # plot the on service rate for w/r
        trace_df.loc[:, 'onservicetime'] = trace_df.onservicetime.astype(int)
        trace_df.loc[:, 'idx'] = trace_df.idx.astype(int)
        df = trace_df.sort_values(by="onservicetime", ignore_index=True)
        df["time_ms"] = (df.onservicetime/bucket_size).astype(int) + 1 # bucket with size of 1 ms
        r_df=df[df.IOType=='read']
        w_df=df[df.IOType=='write']
        r_rate = r_df.groupby(["time_ms"]).count()
        w_rate = w_df.groupby(["time_ms"]).count()
        r_w_rate_ratio = r_rate.idx/w_rate.idx
        marker = ''
        linestyle = "solid"
        # if '1_to_1' in f:
        #     marker = 'x'
        #     linestyle = "solid"
        # else:
        #     if '_to_1' in f and (not '1_to_1' in f):
        #         linestyle = "dashed"

        axs[0].plot(r_rate.idx, label=f.split("/")[-2], marker=marker, linestyle=linestyle)
        axs[1].plot(w_rate.idx, label=f.split("/")[-2], marker=marker, linestyle=linestyle)
        # plot the number of requets on servive for w/r
        axs[2].plot(r_w_rate_ratio, label=f.split("/")[-2], marker=marker, linestyle=linestyle)

    axs[0].set_ylabel("on service rate")
    axs[0].set_title("read")

    axs[1].set_ylabel("on service rate")
    axs[1].set_title("write")

    axs[2].set_ylabel("read/write ratio")
    axs[2].set_title("ratio")
    axs[2].set_yscale('log')
    # labels = [eval(item.get_text()) for item in axs[2].get_xticklabels()]
    # axs[2].set_xticklabels([ str(int(item*bucket_size/1e6)) for item in labels])

    axs[2].set_xlabel("time bin with size {} ns".format(int(bucket_size)))
    figure.set_size_inches(12, 8)
    plt.legend()
    plt.savefig(os.path.join(log_folder, "onservice_rate.png"))
    return trace_df

bucket_size=1e6
log_folder='/home/labuser/Downloads/MQSim/logs/3us_4096B_50000'
# ret_df = get_statistic_df(log_folder)


# plot_runtime_arrival_rate(log_folder, bucket_size=1e6)
trace_dirt = plot_runtime_throughput(log_folder, bucket_size=bucket_size)
plot_runtime_onservice_rate(log_folder, bucket_size=bucket_size)
plot_throughput_summary(log_folder, bucket_size=bucket_size, quantile=0.3)





# trace_df = trace_df.sort_values(by='idx', ignore_index=True)
# response_df = response_df.sort_values(by='ArrivalTime', ignore_index=True)
# read_response_df = response_df[response_df.IOType==0].reset_index(drop=True)
# write_response_df = response_df[response_df.IOType==1].reset_index(drop=True)
# read_onservice_df = trace_df[trace_df.IOType=='read'].reset_index(drop=True)
# write_onservice_df = trace_df[trace_df.IOType=='write'].reset_index(drop=True)

# waiting_df = read_onservice_df.onservicetime - read_response_df.ArrivalTime
# waiting_ratio = waiting_df/read_response_df.DelayTime 
# y = waiting_ratio[(waiting_ratio>0)&(waiting_ratio<1)]
# figure, axs = plt.subplots(2)
# axs[0].scatter( y.index, y )
# axs[0].set_title("read")
# axs[0].set_ylabel("waiting ratio")

# waiting_df = write_onservice_df.onservicetime - write_response_df.ArrivalTime
# waiting_ratio = waiting_df/write_response_df.DelayTime
# y = waiting_ratio[(waiting_ratio>0)&(waiting_ratio<1)]
# axs[1].scatter(y.index, y)
# axs[1].set_title("write")
# axs[1].set_ylabel("waiting ratio")
# axs[1].set_xlabel("request index")
# figure.set_size_inches(12, 8)
# plt.savefig(os.path.join(log_folder, "wait_ratio.png"))