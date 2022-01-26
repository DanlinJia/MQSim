import os
import multiprocessing
import subprocess
import random
import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib

def number_generator(dist_args: dict):
    if 'dist_name' not in dist_args:
        raise()
    if dist_args['dist_name'] == 'uniform':
        a = dist_args['low']
        b = dist_args['high']
        return random.uniform(a, b)
    elif dist_args['dist_name'] == 'expo':
        mean = dist_args['mean']
        return random.expovariate(1/mean)
    elif dist_args['dist_name'] == 'gamma':
        alpha = dist_args['alpha']
        beta = dist_args['beta']
        return random.gammavariate(alpha, beta)
    elif dist_args['dist_name'] == 'gauss':
        mu = dist_args['mu']
        sigma = dist_args['sigma']
        return random.gauss(mu, sigma)

def number_generator_test():
    dist_args = [0]*4
    fig, axs = plt.subplots(4)
    dist_args[0] = {'dist_name':'uniform', 'low':5, 'high':15}
    dist_args[1] = {'dist_name':'expo', 'mean':10}
    dist_args[2] = {'dist_name':'gamma', 'alpha':10, 'beta':1}
    dist_args[3] = {'dist_name':'guass', 'mu':10, 'sigma':1}
    for i, a in enumerate(dist_args):
        x = []
        for j in range(1000):
            x.append(number_generator(a))
        axs[i].scatter(list(range(1000)), x, label=a['dist_name'])
    plt.legend()
    fig = matplotlib.pyplot.gcf()
    fig.set_size_inches(18.5, 10.5)
    plt.savefig('test.png')


def generator(interarrival_mean, size, num, iotype, volume_id=0, target_id=0):
    # generate a df for a certain type of IOs
    df = pd.DataFrame(columns=["ArrivalTime", "VolumeID", "Offset", "Size", "IOType"])
    df["Offset"] = np.random.uniform(low=0, high=10*1024*1024*1024, size=num).astype(int)
    df["VolumeID"] = np.array([volume_id]*num)
    df["InitiatorID"] = df["VolumeID"]
    df["TargetID"] = np.array([target_id]*num)
    df["IOType"] = np.array([iotype]*num)
    size_dist_args = {'dist_name':'expo', 'mean':size}
    interarrival_dist_args = {'dist_name':'expo', 'mean':interarrival_mean}
    df["Size"] = (np.array([number_generator(size_dist_args)/512 for i in range(num)]).astype(int)+ 1)*512
    interarrival_list = np.array([number_generator(interarrival_dist_args) for i in range(num-1)])
    # interarrival_list = np.array([number_generator(interarrival_dist_args) for i in range(num)])
    df["ArrivalTime"] = (np.insert(np.cumsum(interarrival_list), 0, 0)*1e9).astype(int)
    # fake data
    df["DelayTime"] = df["ArrivalTime"]
    df["FinishTime"] = df["ArrivalTime"]
    return df

def generate_trace(interarrival_mean, size, num, output_folder, ratio, name):
    #Type_of_Requests[0 for write, 1 for read]
    df = pd.DataFrame(columns=["ArrivalTime", "VolumeID", "Offset", "Size", "IOType"])
    num_read = int(num*ratio)
    num_write = num - num_read
    read_df = generator(interarrival_mean, size, num_read, 1 )
    write_df = generator(interarrival_mean, size, num_write, 0)
    mix_df = pd.concat([read_df, write_df])
    mix_df = mix_df.sort_values(by=["ArrivalTime"])
    mix_df.index = range(len(mix_df))
    read_df["RequestID"] = np.array(range(len(read_df)))
    write_df["RequestID"] = np.array(range(len(write_df)))
    mix_df["RequestID"] = np.array(range(len(mix_df)))
    path = os.path.join(output_folder, name)
    if not os.path.exists(output_folder):
        os.system("mkdir -p {}".format(output_folder))
    mix_df.to_csv(path, header=True, index=False, sep=",")
    return name
    # names = []
    # for (prefix, df) in [('mix_', mix_df), ('read_', read_df), ('write_', write_df)]:
    #     path = os.path.join(output_folder, prefix+name)
    #     if not os.path.exists(path):
    #         os.system("mkdir -p {}".format(output_folder))
    #     df.to_csv(path, header=True, index=False, sep=",")
    #     names.append(prefix+name)
    # return names

# if __name__ == "__main__":
#     experiment_name = "test"
#     interarrival_mean = 1e-5 #10 us
#     size = 4096
#     num = 100000
#     # df = generator(interarrival_mean, size, num, iotype, volume_id=0, target_id=0)
#     output_folder = "traces/{}".format(experiment_name)
#     if not os.path.exists(output_folder):
#         os.system("mkdir -p {}".format(output_folder))
#     generate_trace(interarrival_mean, size, num, output_folder, ratio=0.5)
