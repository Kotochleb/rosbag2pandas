import argparse
from collections import OrderedDict
from functools import partial
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import Union

import pandas as pd
import rosbag2_py
from rclpy.serialization import deserialize_message
from rosidl_runtime_py import message_to_ordereddict
from rosidl_runtime_py.utilities import get_message
from tqdm import tqdm


def parse_data(message_type: type, data: bytes) -> OrderedDict:
    """Deserializes and converts it to ordered dictionary.

    Args:
        message_type (type): Type of the ROS 2 message.
        data (bytes): Serialized message.

    Returns:
        OrderedDict: Dictionary with data within the message.
    """
    msg = deserialize_message(data, message_type)
    return message_to_ordereddict(msg)


def save_to_file(
    file_format: str, output_folder: Path, data: dict[str, Union[str, pd.DataFrame]]
) -> None:
    """Combines the data of a given topic into single dataframe and saves it to a file.

    Args:
        file_format (str): Output file format.
        output_folder (Path): Path to save parsed data.
        data (dict[str, Union[str, pd.DataFrame]]): Dictionary with name of the topic,
            dataframe with parsed messages and dataframe with timestamps of each message.
    """
    topic_name = data["topic_name"]
    parsed_messages = data["parsed_messages"]
    message_stamps = data["message_stamps"]

    df_messages = pd.json_normalize(parsed_messages)
    df_stamps = pd.DataFrame(message_stamps, columns=["timestamp"])
    df_joined = df_stamps.join(df_messages).sort_values(
        by=["timestamp"], ignore_index=True
    )
    file_name = (
        topic_name if topic_name[0] != "/" else topic_name.lstrip("/")
    ).replace("/", "_")

    if file_format == "csv":
        df_joined.to_csv(output_folder / f"{file_name}.csv")
    elif file_format == "parquet":
        df_joined.to_parquet(output_folder / f"{file_name}.parquet")
    elif file_format == "pickle":
        df_joined.to_pickle(output_folder / f"{file_name}.pkl")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "input", help="Input path (folder or filepath) pointing to a bag."
    )

    parser.add_argument(
        "output", help="Output path to which parsed bags should be stored."
    )

    parser.add_argument(
        "-s",
        help="Storage implementation of bag.",
        default="mcap",
        choices=["sqlite3", "mcap"],
    )

    parser.add_argument(
        "--format",
        help="Output file format.",
        default="csv",
        choices=["csv", "parquet", "pickle"],
    )

    parser.add_argument(
        "--cores",
        help="Number of cores used to process data.",
        default=cpu_count(),
    )

    args = parser.parse_args()
    input_folder = Path(args.input)
    output_folder = Path(args.output)
    file_format = args.format
    storage_implementation = args.s
    n_cores = args.cores

    Path(output_folder).mkdir(parents=True, exist_ok=True)

    reader = rosbag2_py.SequentialReader()
    reader.open(
        rosbag2_py.StorageOptions(
            uri=input_folder.absolute().as_posix(), storage_id=storage_implementation
        ),
        rosbag2_py.ConverterOptions("", ""),
    )

    def topic_type_from_name(topic_name: str) -> type:
        for topic_type in reader.get_all_topics_and_types():
            if topic_type.name == topic_name:
                return topic_type.type
        raise ValueError(f"Topic '{topic_name}' was not found in the bag")

    raw_message_data = {}
    message_stamps = {}

    print("Reading the rosbag...")
    message_count = reader.get_metadata().message_count
    with tqdm(total=message_count, mininterval=0.5, leave=True) as pbar:
        while reader.has_next():
            topic, data, timestamp = reader.read_next()
            if not topic in raw_message_data.keys():
                raw_message_data[topic] = {
                    "message_type": get_message(topic_type_from_name(topic)),
                    "raw_data": [],
                }
            if not topic in message_stamps.keys():
                message_stamps[topic] = []

            raw_message_data[topic]["raw_data"].append(data)
            message_stamps[topic].append(timestamp)

            pbar.update(1)
        pbar.update(1)

    parsed_messages = {}

    for topic_name in raw_message_data.keys():
        print(f"Parsing topic '{topic_name}'...")
        with Pool(n_cores) as pool:
            raw_data = raw_message_data[topic_name]["raw_data"]
            message_type = raw_message_data[topic_name]["message_type"]
            partial_parse_data = partial(parse_data, message_type)
            parsed_messages[topic_name] = list(
                tqdm(
                    pool.imap(partial_parse_data, raw_data, chunksize=500),
                    mininterval=0.5,
                    total=len(raw_data),
                    leave=True,
                )
            )

    print(f"Saving topics to files")
    topic_save_data = [
        {
            "topic_name": topic_name,
            "parsed_messages": parsed_messages[topic_name],
            "message_stamps": message_stamps[topic_name],
        }
        for topic_name in parsed_messages.keys()
    ]
    with Pool(n_cores) as pool:
        partial_save_to_file = partial(save_to_file, file_format, output_folder)
        list(
            tqdm(
                pool.imap(partial_save_to_file, topic_save_data),
                mininterval=0.5,
                total=len(topic_save_data),
                leave=True,
            )
        )


if __name__ == "__main__":
    main()
