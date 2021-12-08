set pagination off
set logging file gdb.output
set logging on

break IO_Flow_Base::Submit_io_request
commands 1
    print *request
    continue
end

break Request_Fetch_Unit_NVMe::Fetch_next_request
commands 2    
    print stream_id
    #backtrace
    continue
end

break IO_Flow_Base::NVMe_read_sqe
commands 3
    p/x address
    p read_request_queue_in_memory
    p write_request_queue_in_memory
    #p *(read_request_queue_in_memory[(uint16_t)((address - nvme_queue_pair.Read_sq.Submission_queue_memory_base_address) / sizeof(Submission_Queue_Entry))])
    #backtrace
    continue
end


break IO_Flow_Base.cpp:479
commands 4
    print *req
    print index
    #backtrace
    continue
end

break Input_Stream_Manager_NVMe::Handle_new_arrived_request
commands 5
    p request
    print *request
    #backtrace
    continue
end


break Input_Stream_Manager_NVMe::Handle_serviced_request
commands 6
    p request
    print *request
    #backtrace
    continue
end


break IO_Flow_Base::NVMe_consume_io_request
commands 7
    print *cqe
    print nvme_software_request_queue
    #backtrace
    continue
end



run

set logging off
quit