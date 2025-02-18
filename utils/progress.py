import json
import os
from utils import FileConfig

def save_file_list(input_files, working_folder, status_json, arg_string, script_type, completed_prefixes=None):
    """
    Saves file information and progress to a JSON file.

    Args:
        input_files (list): List of FileConfig objects representing the files being processed.
        working_folder (str): The folder to store the status JSON file.
        status_json (str): The path to the status JSON file.
        arg_string (str): The argument string used in the script.
        script_type (str): The type of script being run.
        completed_prefixes (list, optional): List of completed file prefixes. Defaults to None.

    Returns:
        None
    """
    if not os.path.exists(working_folder):
        os.makedirs(working_folder)
    simple_file_list = []
    for file in input_files:
        simple_file_list.append([file.input_path, file.output_path, file.complete, file.lines_processed, file.error_lines, file.lines_matched])
    if completed_prefixes is None:
        completed_prefixes = []
    else:
        completed_prefixes = sorted([prefix for prefix in completed_prefixes])
    with open(status_json, 'w') as status_json_file:
        output_dict = {
            "args": arg_string,
            "type": script_type,
            "completed_prefixes": completed_prefixes,
            "files": simple_file_list,
        }
        status_json_file.write(json.dumps(output_dict, indent=4))

def load_file_list(status_json):
    """
    Loads file information from the JSON file and recalculates file sizes.

    Args:
        status_json (str): The path to the status JSON file.

    Returns:
        tuple: A tuple containing:
            - input_files (list): List of FileConfig objects.
            - args (str): The argument string used in the script.
            - script_type (str): The type of script being run.
            - completed_prefixes (set): Set of completed file prefixes.
    """
    if os.path.exists(status_json):
        with open(status_json, 'r') as status_json_file:
            output_dict = json.load(status_json_file)
            input_files = []
            for simple_file in output_dict["files"]:
                input_files.append(
                    FileConfig(simple_file[0], simple_file[1], simple_file[2], simple_file[3], simple_file[4], simple_file[5])
                )
            completed_prefixes = set()
            for prefix in output_dict["completed_prefixes"]:
                completed_prefixes.add(prefix)
            return input_files, output_dict["args"], output_dict["type"], completed_prefixes
    else:
        return None, None, None, set()