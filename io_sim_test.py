# from ssd_simulator import *
from io_sim import *
from synthetic_generator_dl import *
import argparse
import pathlib

def run_workload(workload, log_folder):
    ori = pd.read_csv(workload)
    init_arrival = ori.ArrivalTime
    ssd_output = "net-ssd.csv"
    ssd_sim = ssd_simulator("./tmp",
                            workload, ".",
                            ssd_output)
    df = ssd_sim.ssd_simulation_iter(init_arrival)

    if not os.path.exists(log_folder):
        os.system("mkdir -p {}".format(log_folder))
    path = pathlib.PurePath(log_folder)
    df.to_csv(os.path.join(log_folder, "{}_results.csv".format(path.name)))
    os.system("./clear_log.sh {} {}".format(log_folder, "."))
    shutil.copy("/home/labuser/Downloads/MQSim/workload.trace_scenario_1.xml", log_folder)

    real_df = df 
    # real_df = df.sort_values(by="FinishTime")
    # real_df = real_df.loc[int(len(real_df)*1/8):int(len(real_df)*1/8),:]
    r_df=real_df[real_df.IOType==0]
    w_df=real_df[real_df.IOType==1]
    print("read delay: {}, write delay: {}".format(r_df.DelayTime.mean()/1e3, w_df.DelayTime.mean()/1e3))

def ssd_simulation_throughput(df, bin_num):
   
    interval_time = df["FinishTime"].max() -  df["FinishTime"].min()  
    out, bins = pd.cut(df["FinishTime"], bin_num, retbins=True, labels=False)
    size_sum = []
    for i in range(bin_num + 1):
        size_sum.append(df[out == i]["Size"].sum()/(interval_time/bin_num))
    return size_sum

def cdfplot(data, bin_num):
    counts, bin_edges= np.histogram(data, bins=bin_num, normed=False)
    cdf = np.cumsum(counts)
    return bin_edges[1:], cdf / cdf[-1]


if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_name", "-i", default="")
    parser.add_argument("--output_name", "-o", default="")
    parser.add_argument("--interarrival", "-a", default=0, type=float)
    parser.add_argument("--size", "-s", default=0, type=float)
    parser.add_argument("--num", "-n", default=0, type=int)
    parser.add_argument("--ratio", '-r', default=0.5, type=float)
    # parser.add_argument("--dry_run", '-d', default=False, type=bool)
    args = parser.parse_args()

    if len(args.input_name)!=0:
        experiment_name = args.input_name
        output_folder = os.path.join("traces", args.output_name)
        log_folder = os.path.join("logs", args.output_name)
        run_workload(experiment_name, log_folder)
    else:
        interarrival_mean = args.interarrival #s
        size = args.size
        num = args.num
        name_dict = {0.5:"1:1RW", 0.8:"4:1RW", 0.2:"1:4RW", 0:"W", 1:"R", 0.75:"3:1RW", 0.25:"1:3RW", 0.4:"2:3RW", 0.6:"3:2RW"}
        name = "{}_{}us_{}B_{}.csv".format(name_dict[args.ratio], int(interarrival_mean*1e6), int(size), num)
        experiment_name = "{}us_{}B_{}".format(int(interarrival_mean*1e6), int(size), num)
        # df = generator(interarrival_mean, size, num, iotype, volume_id=0, target_id=0)
        output_folder = "traces/{}".format(experiment_name)
        trace_name = generate_trace(interarrival_mean, int(size), num, output_folder, ratio=args.ratio, name=name)
        workload  = os.path.join(output_folder, trace_name)
        log_folder = os.path.join("logs", experiment_name, trace_name.split('.')[0])
        run_workload(workload, log_folder)


    # filename = "test"
    # workload = "/home/labuser/ssd-net-sim/traces/net-ssd-Test/"+ filename + ".csv"
    # workload = "/home/labuser/Downloads/MQSim/traces/test/mix_10us_4096B_100000.csv"
    


    # df_sum = 0
    # for i in range(len(df-1)):
    #     #df_sum = df_sum + (df.FinishTime.iloc[i] - df.ArrivalTime.iloc[i])#durantion 
    #     df_size = df.Size.sum()
    # df_sum = df.FinishTime.iloc[len(df)-1]-df.ArrivalTime.iloc[0]
    # print("overall throuput:" , (df_size/1e9)/(df_sum/1e9))

    # #write througput 
    # w_df_sum = 0
    # for i in range(len(w_df-1)):
    #     #w_df_sum = w_df_sum + (w_df.FinishTime.iloc[i] - w_df.ArrivalTime.iloc[i])
    #     w_df_size =  w_df.Size.sum()
    # w_df_sum = w_df.FinishTime.iloc[len(w_df)-1]-w_df.ArrivalTime.iloc[0]
    # print("write throuput:" , w_df_size/w_df_sum)   


    # #read througput 
    # r_df_sum = 0
    # for i in range(len(r_df-1)):
    #     #r_df_sum = r_df_sum + (r_df.FinishTime.iloc[i] - r_df.ArrivalTime.iloc[i])
    #     r_df_size = r_df.Size.sum()
    # r_df_sum = r_df.FinishTime.iloc[len(r_df)-1]-r_df.ArrivalTime.iloc[0]
    # print("read throuput:" , r_df_size/r_df_sum)
    
    # print ("AveDelay:", df.DelayTime.mean())
    # bin_num = 100
    # thr = ssd_simulation_throughput(ori, bin_num)
    # edges, cdf = cdfplot(thr, bin_num)
    
    # plt.figure()
    # plt.xlabel("Unit time ")
    # plt.ylabel("Throughput [Gbps]")
    # plt.plot(thr)
    # plt.savefig(filename + "_thr.png")
    # plt.figure()
    # plt.xlabel("Throughput [Gbps]")
    # plt.hist(thr)
    # plt.savefig(filename + "_pdf.png")
    # plt.figure()
    # #cdf
    # plt.xlabel("Throughput [Gbps]")
    # plt.ylabel("Probility")
    # plt.plot(edges, cdf)
    # plt.savefig(filename + "_cdf.png")
    




    # def ssd_simulation_iter(self, arrival_time):
    #     intermedia_file = self.net_out_trace
    #     intermedia_path = self.ssd_in_trace
    #     now = datetime.now()
    #     time = now.strftime("%H:%M:%S")
    #     os.system("cp {} {}".format(intermedia_file, intermedia_file+"_"+time))
    #     net_df = pd.read_csv(intermedia_file, header=0)
    #     trace_df = net_df[["RequestID","ArrivalTime", "InitiatorID", "Offset", "Size", "IOType"]]
    #     trace_df = trace_df.sort_values(by=["ArrivalTime"])
    #     trace_df.loc[:, "Size"] = trace_df.Size.apply(lambda x: int(x/512))
    #     trace_df.drop(["RequestID"], axis=1).to_csv(path_or_buf=intermedia_path, sep=" ", header=False, index=False)
    #     new_df, distance = self.run_SSD_sim(intermedia_path, self.output_folder, self.output_path, trace_df = trace_df, old_df = net_df)
    #     new_df = net_df[["RequestID", "InitiatorID", "TargetID"]].merge(new_df, left_on="RequestID", right_on="RequestID")
    #     self.distances["DelayTime"].append(distance["DelayTime"])
    #     self.distances["FinishTime"].append(distance["FinishTime"])
    #     new_df.loc[:, "ArrivalTime"] = arrival_time
    #     new_df.to_csv(path_or_buf=self.output_path, sep=",", header=True, index=False)
    #     names = ["RequestID", "ArrivalTime", "DelayTime", "FinishTime", "InitiatorID", "TargetID", "IOType", "Size", "InitiatorID", "Offset"]
    #     return new_df[names]