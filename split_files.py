import os


def split_files(input_files, number_of_splits):
    counts_of_lines = dict()

    splits = {i: dict() for i in range(0, number_of_splits)}

    for input_file in input_files:
        with open(input_file, 'r') as file_obj:
            count = sum(1 for _, line in enumerate(file_obj))
            counts_of_lines[input_file] = count

    number_of_lines = sum(list(counts_of_lines.values()))

    lines_per_split = int(number_of_lines / number_of_splits)
    rest_of_lines = number_of_lines % number_of_splits

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

    for input_file in input_files:
        file_segments_shifts[input_file] = shift
        shift += counts_of_lines[input_file]

    for s in range(0, number_of_splits):
        split_range = split_ranges[s]
        for input_file in input_files:
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


def split_files(self):
    input_files = self.map_reduce_specification.input_files
    counts_of_lines = dict()

    for input_file in input_files:
        with open(input_file, 'r') as file_obj:
            count = sum(1 for _, line in enumerate(file_obj))
            counts_of_lines[input_file] = count

    number_of_lines = sum(list(counts_of_lines.values()))
    lines_per_split = int(floor(number_of_lines / self.number_of_splits))

    splits = {i: dict() for i in range(0, self.number_of_splits)}

    current_file_number = 0
    beg_position = 0

    for s in range(0, self.number_of_splits):
        lines_number_in_split = 0

        if s == self.number_of_splits - 1:
            current_file = input_files[current_file_number]
            lines_number = counts_of_lines[current_file] - 1

            if beg_position != lines_number:
                splits[s][current_file] = (beg_position, lines_number)

            break

        while lines_number_in_split < lines_per_split:
            current_file = input_files[current_file_number]
            lines_in_file = counts_of_lines[current_file]

            if lines_number_in_split + lines_in_file <= lines_per_split:
                splits[s][current_file] = (beg_position, beg_position + lines_in_file - 1)
                current_file_number += 1
                beg_position = 0
                lines_number_in_split += lines_in_file
            elif lines_number_in_split + lines_in_file > lines_per_split:
                splits[s][current_file] = (beg_position, beg_position + lines_per_split - lines_number_in_split - 1)
                beg_position += lines_per_split - lines_number_in_split
                lines_number_in_split += (lines_per_split - lines_number_in_split)

    return splits


splits = split_files([f'L2/test.txt', f'L2/test1.txt', f'L2/test2.txt'], 8)
print(splits)
