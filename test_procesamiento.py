import os
import sys
import time
import argparse
import re
import logging.handlers
import multiprocessing
from utils import setup_logging, FileConfig, Queue, save_file_list, load_file_list, process_file

# Setup the logger to both the console and file
log = setup_logging()




# Conectar a la base de datos

 
if __name__ == '__main__':
	parser = argparse.ArgumentParser(description="Use multiple processes to decompress and iterate over pushshift dump files")
	parser.add_argument("input", help="The input folder to recursively read files from")
	parser.add_argument("--output", help="Put the output files in this folder", default="")
	parser.add_argument("--working", help="The folder to store temporary files in", default="temp_files")
	parser.add_argument("--field", help="When deciding what lines to keep, use this field for comparisons", default="subreddit")
	parser.add_argument("--value", help="When deciding what lines to keep, compare the field to this value. Supports a comma separated list. This is case sensitive", default="mentalhealth")
	parser.add_argument("--processes", help="Number of processes to use", default=10, type=int)
	parser.add_argument("--debug", help="Enable debug logging", action='store_const', const=True, default=False)
	parser.add_argument("--start_date", help="Start date in YYYY-MM format", default=None)
	parser.add_argument("--end_date", help="End date in YYYY-MM format", default=None)
	parser.add_argument("--database", help="The database to write to", default="reddit_bc")
	parser.add_argument("--comment_depth", help="The maximum depth of comments to process (-1 no comments, 0 comments whose parent is the post, 1 comments whose parent is a comment with 0 depth and so on.)", type=int)
	script_type = "split"

	args = parser.parse_args()
	arg_string = f"{args.field}:{(args.value if args.value else None)}"

	if args.debug:
		log.setLevel(logging.DEBUG)

	log.info(f"Loading files from: {args.input}")
	if args.output:
		log.info(f"Writing output to: {args.output}")
	else:
		log.info(f"Writing output to working folder")

	values = set()
	values = set(args.value.split(","))

	
	lower_values = set()
	for value_inner in values:
		lower_values.add(value_inner.strip().lower())
	values = lower_values
	if len(values) > 5:
		val_string = f"any of {len(values)} values"
	elif len(values) == 1:
		val_string = f"the value {(','.join(values))}"
	else:
		val_string = f"any of the values {(','.join(values))}"
	log.info(f"Checking if any of {val_string} exactly match field {args.field}")

	multiprocessing.set_start_method('spawn')
	queue = multiprocessing.Manager().Queue()
	status_json = os.path.join(args.working, "status.json")
	input_files, saved_arg_string, saved_type, completed_prefixes = load_file_list(status_json)
	if saved_arg_string and saved_arg_string != arg_string:
		log.warning(f"Args don't match args from json file. Delete working folder")
		sys.exit(0)

	if saved_type and saved_type != script_type:
		log.warning(f"Script type doesn't match type from json file. Delete working folder")
		sys.exit(0)

	# First time running, build the list of files to process
	if input_files is None:
		input_files = []
		for subdir, dirs, files in os.walk(args.input):
			files.sort()
			for file_name in files:
				if file_name.endswith(".zst") and re.search("^RC_|^RS_", file_name) is not None:
					file_date = re.search(r"(\d{4}-\d{2})", file_name)
					if file_date:
						file_date = file_date.group(1)
						if (args.start_date and file_date < args.start_date) or (args.end_date and file_date > args.end_date):
							continue
					input_path = os.path.join(subdir, file_name)
					output_extension = ".zst"
					output_path = os.path.join(args.working, f"{file_name[:-4]}{output_extension}")
					input_files.append(FileConfig(input_path, output_path=output_path))

		save_file_list(input_files, args.working, status_json, arg_string, script_type)
	else:
		log.info(f"Existing input file was read, if this is not correct you should delete the {args.working} folder and run this script again")
  
	log.info(f"Filtered {len(input_files)} files based on the provided date range {args.start_date if args.start_date else 'None'} to {args.end_date if args.end_date else 'None'}")
 
	if len(input_files) == 0:
		log.info("No files to process, exiting")
		sys.exit(0)

	files_processed, total_bytes, total_bytes_processed, total_lines_processed, total_lines_matched, total_lines_errored = 0, 0, 0, 0, 0, 0
	files_to_process = []
 
	# Calculate the size for progress reports
	for file in sorted(input_files, key=lambda item: item.file_size, reverse=True):
		total_bytes += file.file_size
		if file.complete:
			files_processed += 1
			total_lines_processed += file.lines_processed
			total_lines_matched += file.lines_matched
			total_bytes_processed += file.file_size
			total_lines_errored += file.error_lines
		else:
			files_to_process.append(file)

	log.info(f"Processed {files_processed} of {len(input_files)} files with {(total_bytes_processed / (2**30)):.2f} of {(total_bytes / (2**30)):.2f} gigabytes")

	start_time = time.time()
	if len(files_to_process):
		progress_queue = Queue(40)
		progress_queue.put([start_time, total_lines_processed, total_bytes_processed])
		speed_queue = Queue(40)
		for file in files_to_process:
			log.info(f"Processing file: {file.input_path}")
   
		# Start the threads
		with multiprocessing.Pool(processes=min(args.processes, len(files_to_process))) as pool:
			log.info(f"Starting {len(files_to_process)} files with {args.processes} processes")
			workers = pool.starmap_async(process_file, [(file, queue, args.field, values, args.database, args.comment_depth) for file in files_to_process], chunksize=1, error_callback=log.info)
			while not workers.ready() or not queue.empty():
				
    			# Loop through the updates from the workers
				file_update = queue.get()
				if file_update.error_message is not None:
					log.warning(f"File failed {file_update.input_path}: {file_update.error_message}")

				# Debug message if the file is just starting
				if file_update.lines_processed == 0:
					log.debug(f"Starting file: {file_update.input_path} : {file_update.file_size:,}")
					continue

				total_lines_processed, total_lines_matched, total_bytes_processed, total_lines_errored, files_processed, files_errored, i = 0, 0, 0, 0, 0, 0, 0
				for file in input_files:
					if file.input_path == file_update.input_path:
						input_files[i] = file_update
						file = file_update
					total_lines_processed += file.lines_processed
					total_lines_matched += file.lines_matched
					total_bytes_processed += file.bytes_processed
					total_lines_errored += file.error_lines
					files_processed += 1 if file.complete or file.error_message is not None else 0
					files_errored += 1 if file.error_message is not None else 0
					i += 1
				if file_update.complete or file_update.error_message is not None:
					save_file_list(input_files, args.working, status_json, arg_string, script_type)
					log.debug(f"Finished file: {file_update.input_path} : {file_update.file_size:,}")
				current_time = time.time()
				progress_queue.put([current_time, total_lines_processed, total_bytes_processed])

				first_time, first_lines, first_bytes = progress_queue.peek()
				bytes_per_second = int((total_bytes_processed - first_bytes)/(current_time - first_time))
				speed_queue.put(bytes_per_second)
				seconds_left = int((total_bytes - total_bytes_processed) / int(sum(speed_queue.list) / len(speed_queue.list)))
				minutes_left = int(seconds_left / 60)
				hours_left = int(minutes_left / 60)
				days_left = int(hours_left / 24)

				log.info(
					f"{total_lines_processed:,} lines at {(total_lines_processed - first_lines)/(current_time - first_time):,.0f}/s, {total_lines_errored:,} errored, {total_lines_matched:,} matched : "
					f"{(total_bytes_processed / (2**30)):.2f} gb at {(bytes_per_second / (2**20)):,.0f} mb/s, {(total_bytes_processed / total_bytes) * 100:.0f}% : "
					f"{files_processed}({files_errored})/{len(input_files)} files : "
					f"{(str(days_left) + 'd ' if days_left > 0 else '')}{hours_left - (days_left * 24)}:{minutes_left - (hours_left * 60):02}:{seconds_left - (minutes_left * 60):02} remaining")
        


	log.info(f"Total time: {int(time.time() - start_time)} seconds : {total_lines_matched:,} matched/{total_lines_processed:,} total lines")
