import os
import multiprocessing
import subprocess
import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt

from ssd_simulator import *

def generator(interarrival_mean, size, num, iotype, volume_id, target_id):
    df = pd.DataFrame(columns=["ArrivalTime", "VolumeID", "Offset", "Size", "IOType"])
    df["Offset"] = np.random.uniform(low=0, high=10*1024*1024*1024, size=num).astype(int)
    df["VolumeID"] = np.array([volume_id]*num)
    df["InitiatorID"] = df["VolumeID"]
    df["TargetID"] = np.array([target_id]*num)
    df["IOType"] = np.array([iotype]*num)
    df["Size"] = (np.random.uniform(size*0.8, size*1.2, num)/512).astype(int) + 1
    interarrival_list = np.random.uniform(interarrival_mean*0.8, interarrival_mean*1.2, num-1)
    interarrival_list = np.random.normal(interarrival_mean, interarrival_mean*0.2, num-1)
    df["ArrivalTime"] = (np.insert(np.cumsum(interarrival_list), 0, 0)*1e9).astype(int)
    return df

def generate_trace(interarrival_mean, size, num, output_folder, ratio=0.5):
    #Type_of_Requests[0 for write, 1 for read]
    df = pd.DataFrame(columns=["ArrivalTime", "VolumeID", "Offset", "Size", "IOType"])
    num_read = int(num*ratio)
    num_write = num - num_read
    read_df = generator(interarrival_mean, size, num_read, 1 )
    write_df = generator(interarrival_mean, size, num_write, 0)
    mix_df = pd.concat([read_df, write_df])
    mix_df = mix_df.sort_values(by=["ArrivalTime"])
    mix_df.index = range(len(mix_df))
    name = "{}us_{}B_{}".format(int(interarrival_mean*1e6), size, num)
    names = []
    for (prefix, df) in [('mix_', mix_df), ('read_', read_df), ('write_', write_df)]:
        path = os.path.join(output_folder, prefix+name)
        df.to_csv(path, header=False, index=False, sep=" ")
        names.append(prefix+name)
    return names

def get_request_time_from_log(statistic_path):
    tree = et.parse(statistic_path)
    read_overall = tree.find('SSDDevice/SSDDevice.HostInterface/SSDDevice.IO_Stream/Average_Read_Transaction_Turnaround_Time').text
    read_execution = tree.find('SSDDevice/SSDDevice.HostInterface/SSDDevice.IO_Stream/Average_Read_Transaction_Execution_Time').text
    read_wait = tree.find('SSDDevice/SSDDevice.HostInterface/SSDDevice.IO_Stream/Average_Read_Transaction_Waiting_Time').text
    read_transfer = tree.find('SSDDevice/SSDDevice.HostInterface/SSDDevice.IO_Stream/Average_Read_Transaction_Transfer_Time').text
    write_overall = tree.find('SSDDevice/SSDDevice.HostInterface/SSDDevice.IO_Stream/Average_Write_Transaction_Turnaround_Time').text
    write_execution = tree.find('SSDDevice/SSDDevice.HostInterface/SSDDevice.IO_Stream/Average_Write_Transaction_Execution_Time').text
    write_wait = tree.find('SSDDevice/SSDDevice.HostInterface/SSDDevice.IO_Stream/Average_Write_Transaction_Waiting_Time').text
    write_transfer = tree.find('SSDDevice/SSDDevice.HostInterface/SSDDevice.IO_Stream/Average_Write_Transaction_Transfer_Time').text
    row = [read_overall,read_execution, read_wait, read_transfer, write_overall, write_execution, write_wait, write_transfer]
    return row

def analyze_log(names):
    df = pd.DataFrame(columns=["test_name", "iotype", "interarrival_mean", "size" ,"read_all", "read_exec", "read_wait", "read_trans", "write_all", "write_exec", "write_wait", "write_trans"])
    for i in range(len(names)):
        name = names[i]
        iotype, inter_mean, size = [name.split("_")[0], int(name.split("_")[1][:-2]), int(name.split("_")[2][:-1])]
        statistic_path = os.path.join(output_folder,"statistic_"+name)
        row = get_request_time_from_log(statistic_path)
        df.loc[i, :] = np.array([name, iotype, inter_mean, size]+row)
    return df


if __name__ == "__main__":
    output_folder = '/home/labuser/Downloads/disaggregate-storage-simulator/traces/ssd-net-synthetic'
    name = '10us_16KB_10:1_x2'
    output_file = os.path.join(output_folder, "{}_net-ssd.csv".format(name))
    targetid = 0
    path = os.path.join(output_folder, name+".csv")
    reads=[]
    writes =[]
    for i in range(10):
        reads.append(generator(0.001, 16000, 5000, 1, i, targetid ))

    for i in range(10):
        writes.append(generator(0.001, 16000, 5000, 0, i, targetid))

    df = pd.concat(reads+writes)
    df = df.sort_values(by=["ArrivalTime"])
    df = df.reset_index()
    df["RequestID"] = df.index
    trace_df = df[["ArrivalTime", "VolumeID", "Offset", "Size", "IOType"]]
    trace_df.loc[:10000,:].to_csv(path, header=False, index=False, sep=" ")
    response_file = run_MQSim(path, output_folder)
    response_df = get_response_df(targetid)
    output_df = pd.concat([df.loc[:10000,:], response_df], axis=1)

    output_df["FinishTime"] = output_df.ArrivalTime + output_df.DelayTime
    output_df["RequestID"] = output_df.index
    output_df.loc[:, "Size"] = output_df.Size.apply(lambda x: x*512)
    names = ["RequestID", "ArrivalTime", "DelayTime", "FinishTime", "InitiatorID", "TargetID", "IOType", "Size", "VolumeID", "Offset"]
    output_df[names].loc[:10000,:].to_csv(path_or_buf=output_file, sep=",", header=True, index=False)

    def plot_actual_cast_ratio(trace_df, output_folder):
        trace_df["Time(ms)"] = (trace_df.ArrivalTime / 1e6).astype(int)
        df = trace_df.groupby(["Time(ms)"]).count()
        df = trace_df.groupby(["Time(ms)"]).agg({"VolumeID": "nunique"})
        ser = pd.Series(df.VolumeID)
        ser.hist(cumulative=True, density=1, bins=1000)
        plt.savefig(output_folder+"/count.png")
    plot_actual_cast_ratio(output_df, output_folder)

# output_folder = "/home/labuser/Downloads/disaggregate-storage-simulator/traces/interference"
# names = []

# # for inter_mean in [0.001, 0.0001, 0.00001, 0.000001]:
# #     outputs = generate_trace(interarrival_mean=inter_mean, size=512*8, num=10000, output_folder=output_folder ,ratio=0.5)
# #     names.extend(outputs)

# for sector in [1, 2, 4, 8, 16, 32, 64, 128]:
#     outputs = generate_trace(interarrival_mean=0.0001, size=512*sector, num=10000, output_folder=output_folder ,ratio=0.5)
#     names.extend(outputs)

# for name in names:
#     run_MQSim(os.path.join(output_folder,name+".csv"), output_folder)

# df = analyze_log(names)

