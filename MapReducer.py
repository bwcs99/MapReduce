import multiprocessing as mp
import os


class MapReducer:
    def __init__(self, map_reduce_specification_object, number_of_splits):
        self.mapper_function = None
        self.reducer_function = None
        self.hash_function = None
        self.map_reduce_specification = map_reduce_specification_object
        self.map_workers_number = 5
        self.reducer_workers_number = 5
        self.number_of_splits = number_of_splits

    def set_mapper_function(self, mapper_function):
        self.mapper_function = mapper_function

    def set_reducer_function(self, reducer_function):
        self.reducer_function = reducer_function

    def set_hash_function(self, hash_func):
        self.hash_function = hash_func

    def set_map_reduce_specification(self, map_reduce_specification):
        self.map_reduce_specification = map_reduce_specification

    def set_map_workers_number(self, number_of_map_workers):
        self.map_workers_number = number_of_map_workers

    def set_reduce_workers_number(self, number_of_reduce_workers):
        self.reducer_workers_number = number_of_reduce_workers

    def get_mapper_function(self):
        return self.mapper_function

    def get_reducer_function(self):
        return self.reducer_function

    def get_map_reduce_specification(self):
        return self.map_reduce_specification

    def get_map_workers_number(self):
        return self.map_workers_number

    def get_reduce_workers_number(self):
        return self.reducer_workers_number

    def compute_reducer_partition_for_given_key(self, key):
        return self.hash_function(key) % self.reducer_workers_number

    def split_files(self):
        counts_of_lines = dict()

        splits = {i: dict() for i in range(0, self.number_of_splits)}

        for input_file in self.map_reduce_specification.input_files:
            with open(input_file, 'r') as file_obj:
                count = sum(1 for _, line in enumerate(file_obj))
                counts_of_lines[input_file] = count

        number_of_lines = sum(list(counts_of_lines.values()))

        lines_per_split = int(number_of_lines / self.number_of_splits)
        rest_of_lines = number_of_lines % self.number_of_splits

        split_ranges = [(i, i + lines_per_split - 1) for i in range(0, number_of_lines, lines_per_split)]

        if rest_of_lines != 0:
            last_pair = split_ranges.pop()

            beg, end = last_pair

            last_pair = beg, beg + rest_of_lines - 1

            split_ranges.append(last_pair)

        first_last_pair = split_ranges.pop()
        second_last_pair = split_ranges.pop()

        second_last_pair_beg = second_last_pair[0]
        first_last_pair_end = first_last_pair[1]

        last_pair = (second_last_pair_beg, first_last_pair_end)

        split_ranges.append(last_pair)

        line_sum = 0

        file_lines_ranges = dict()

        for k, v in counts_of_lines.items():
            file_lines_ranges[k] = (line_sum, line_sum + v - 1)
            line_sum += v

        file_segments_shifts = dict()
        shift = 0

        for input_file in self.map_reduce_specification.input_files:
            file_segments_shifts[input_file] = shift
            shift += counts_of_lines[input_file]

        for s in range(0, self.number_of_splits):
            split_range = split_ranges[s]
            for input_file in self.map_reduce_specification.input_files:
                file_segment_range = file_lines_ranges[input_file]
                shift_for_file_segment = file_segments_shifts[input_file]

                file_segment_beg, file_segment_end = file_segment_range
                split_range_beg, split_range_end = split_range

                if split_range_beg <= file_segment_beg and file_segment_end <= split_range_end:
                    splits[s][input_file] = (file_segment_beg - shift_for_file_segment,
                                             file_segment_end - shift_for_file_segment)
                elif split_range_beg <= file_segment_beg <= split_range_end <= file_segment_end:
                    splits[s][input_file] = (file_segment_beg - shift_for_file_segment,
                                             split_range_end - shift_for_file_segment)
                elif file_segment_beg <= split_range_beg <= file_segment_end <= split_range_end:
                    splits[s][input_file] = (split_range_beg - shift_for_file_segment,
                                             file_segment_end - shift_for_file_segment)
                elif file_segment_beg <= split_range_beg and split_range_end <= file_segment_end:
                    splits[s][input_file] = (split_range_beg - shift_for_file_segment,
                                             split_range_end - shift_for_file_segment)

        return splits

    def master_process_function(self, file_splits, tasks_queues, master_queue, mq, eq):
        processed_splits = {list(file_splits.keys())[i]: False for i in range(0, len(list(file_splits.keys())))}

        tasks_dict = dict()
        pid_to_task_queue = dict()
        state_dict = dict()

        reduce_tasks_numbers = dict()
        reduce_tasks_counter = 0

        map_reduce_poll = ['map'] * self.map_workers_number + ['reduce'] * self.reducer_workers_number
        mq.put(f'Proces master rozpoczyna działanie !')

        while True:
            message_tuple = master_queue.get()

            header = message_tuple[0]

            if header == f'PID_INFO':
                task_type = map_reduce_poll.pop(0)

                process_pid = message_tuple[1]
                task_queue_id = message_tuple[2]

                pid_to_task_queue[process_pid] = task_queue_id

                tasks_dict[process_pid] = task_type
                state_dict[process_pid] = f'IDLE'

                mq.put(f'Otrzymałem wiadomość od {process_pid}. Typ przydzielonego zadania: {task_type.capitalize()}')

                if task_type == f'reduce':
                    reduce_tasks_numbers[reduce_tasks_counter] = process_pid
                    reduce_tasks_counter += 1

                if len(map_reduce_poll) == 0:
                    mq.put(f'Wszystkie procesy zostały zidentyfikowane - rozdzielam zadania map !')

                    for queue in tasks_queues:
                        queue.put((f'OK',))

                    for pid, task in tasks_dict.items():
                        if task == f'map':
                            for key, value in processed_splits.items():
                                if not value:
                                    task_queue_id = pid_to_task_queue[pid]
                                    tasks_queues[task_queue_id].put(('map', file_splits[key], self.mapper_function))
                                    state_dict[pid] = f'IN PROGRESS'
                                    processed_splits[key] = True
                                    break

            elif header == f'MAP TASK DONE':
                process_pid, data_locations = message_tuple[1], message_tuple[2]

                task_queue_id = pid_to_task_queue[process_pid]

                process_termination_flag = True

                state_dict[process_pid] = f'IDLE'

                for i in range(0, self.reducer_workers_number):
                    reduce_task_pid = reduce_tasks_numbers[i]
                    reduce_task_message_queue = pid_to_task_queue[reduce_task_pid]

                    data_location_for_task = data_locations[i]

                    tasks_queues[reduce_task_message_queue].put((f'reduce', data_location_for_task))

                for key, value in processed_splits.items():
                    if not value:
                        process_termination_flag = False
                        tasks_queues[task_queue_id].put(('map', file_splits[key], self.mapper_function))
                        processed_splits[key] = True
                        state_dict[process_pid] = f'IN PROGRESS'
                        break

                if process_termination_flag:
                    tasks_queues[task_queue_id].put((f'end',))

                    state_dict[process_pid] = f'TERMINATED'

            elif header == f'REDUCE TASK DONE':
                process_id = message_tuple[1]

                mq.put(f'Dostałem REDUCE TASK DONE od {process_id}')

                task_queue_id = pid_to_task_queue[process_id]

                tasks_queues[task_queue_id].put((f'end',))

                state_dict[process_id] = f'TERMINATED'

            if len(set(list(state_dict.values()))) == 1 and list(state_dict.values())[0] == f'TERMINATED':
                mq.put(f'Kończymy proces map reduce !')
                eq.put((f'end',))
                break

    def worker_process_function(self, task_queue, master_queue, mq, task_queue_id):
        first_time = True
        first_map_task = True
        i_am_reduce_task = False

        data_locations_buffer = []

        mq.put(f'Proces {os.getpid()} działa !')

        while True:
            if first_time:
                mq.put(f'Wysyłam do mastera moje PID: {os.getpid()}')

                master_queue.put((f'PID_INFO', os.getpid(), task_queue_id))

                first_time = False

                _ = task_queue.get()

            task_from_master = task_queue.get()

            mq.put(f'Proces {os.getpid()} otrzymał następujące zadanie: {task_from_master}')

            task_kind = task_from_master[0]

            if task_kind == f'map':
                map_results_buffer = []

                if first_map_task:
                    for i in range(0, self.reducer_workers_number):
                        f = open(f'{os.getpid()}R{i}.txt', 'w')
                        f.close()

                assigned_splits = task_from_master[1]

                split_contents = []

                for file, selected_lines in assigned_splits.items():
                    lower_bound, upper_bound = selected_lines

                    with open(file) as file_obj:
                        for number_of_line, line in enumerate(file_obj):
                            if lower_bound <= number_of_line <= upper_bound:
                                split_contents.append((file, str(line)))

                for pair in split_contents:
                    key_value_pairs_from_mapper = self.mapper_function(*pair)

                    map_results_buffer += key_value_pairs_from_mapper

                    if len(map_results_buffer) >= 15:
                        for key_value_pair in map_results_buffer:
                            current_key = key_value_pair[0]
                            reducer_partition_number = self.compute_reducer_partition_for_given_key(current_key)

                            with open(f'{os.getpid()}R{reducer_partition_number}.txt', 'a') as file_obj:
                                file_obj.write(str(key_value_pair) + '\n')

                        map_results_buffer.clear()

                if len(map_results_buffer) > 0:
                    for key_value_pair in map_results_buffer:
                        current_key = key_value_pair[0]
                        reducer_partition_number = self.compute_reducer_partition_for_given_key(current_key)

                        with open(f'{os.getpid()}R{reducer_partition_number}.txt', 'a') as file_obj:
                            file_obj.write(str(key_value_pair) + '\n')

                master_queue.put(('MAP TASK DONE', os.getpid(), {i: f'{os.getpid()}R{i}.txt'
                                                                 for i in range(0, self.reducer_workers_number)},
                                  assigned_splits))

                first_map_task = False

                mq.put(f'Proces {task_queue_id} zakończył zadanie MAP.')

            elif task_kind == f'reduce':
                i_am_reduce_task = True
                data_location = task_from_master[1]

                data_locations_buffer.append(data_location)

                data_locations_buffer = list(set(data_locations_buffer))

                if len(data_locations_buffer) == self.map_workers_number:
                    file_obj = open(f'R{os.getpid()}Result.txt', 'w')
                    file_obj.close()

                    intermediate_key_value_pairs = []

                    for intermediate_data_location in data_locations_buffer:
                        with open(intermediate_data_location) as file_obj:
                            for number_of_line, line in enumerate(file_obj):
                                line_to_pair = eval(line)
                                intermediate_key_value_pairs.append(line_to_pair)

                    intermediate_key_value_pairs.sort(key=lambda x: x[0])

                    set_of_keys = {k[0] for k in intermediate_key_value_pairs}

                    intermediate_values_dict = {k: [kv[1] for kv in intermediate_key_value_pairs if kv[0] == k] for
                                                k in set_of_keys}

                    for key, value in intermediate_values_dict.items():
                        result_value = self.reducer_function(key, value)

                        with open(f'R{os.getpid()}Result.txt', 'a') as file_obj:
                            file_obj.write(str(result_value) + f'\n')

                    master_queue.put((f'REDUCE TASK DONE', os.getpid()))

                    mq.put(f'Proces {os.getpid()} zakończył zadanie REDUCE !')

            elif task_kind == f'end':
                mq.put(f'Proces {os.getpid()} kończy działanie !')
                if i_am_reduce_task:

                    for data_location in data_locations_buffer:
                        os.remove(data_location)

                mq.put(f'Worker {task_queue_id} kończy działanie !')

                break

    def map_reduce(self):
        if self.mapper_function is None or self.reducer_function is None:
            print(f'Nie podano funkcji mappera lub reducera ! Nie można rozpocząć procesu MAP REDUCE !')
            return

        if self.hash_function is None:
            print(f'Błąd ! Nie podano funkcji haszującej !')
            return

        file_splits_locations = self.split_files()

        workers = []

        tasks_queues = []
        master_queue = mp.Queue()
        mq = mp.Queue()

        end_queue = mp.Queue()

        for i in range(0, self.map_workers_number + self.reducer_workers_number):
            task_queue = mp.Queue()

            tasks_queues.append(task_queue)

            worker_process = mp.Process(target=self.worker_process_function, args=(task_queue, master_queue, mq, i))

            workers.append(worker_process)

        master_process = mp.Process(target=self.master_process_function, args=(file_splits_locations, tasks_queues,
                                                                               master_queue, mq, end_queue))

        master_process.start()

        for worker in workers:
            worker.start()

        message_server_process = mp.Process(target=self.message_server, args=(mq,))

        message_server_process.start()

        end_queue.get()

    def message_server(self, message_queue):
        while True:
            message = message_queue.get()
            print(message)

            if message == f'Kończymy proces map reduce !':
                break
