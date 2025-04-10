from pathlib import Path
from typing import List

from setuptools import setup

package_name = "rosbag2pandas"
project_source_dir = Path(__file__).parent


def get_files(dir: Path, pattern: str) -> List[str]:
    return [x.as_posix() for x in (dir).glob(pattern) if x.is_file()]


setup(
    name=package_name,
    version="0.0.0",
    packages=[package_name],
    package_dir={},
    install_requires=["setuptools"],
    zip_safe=True,
    data_files=[
        (
            "share/ament_index/resource_index/packages/resource",
            get_files(project_source_dir / "resource", "*"),
        ),
        ("share/" + package_name, ["package.xml"]),
    ],
    maintainer="Krzysztof Wojciechowski",
    maintainer_email="krzy.wojciecho@gmail.com",
    description="Tool to convert rosbags from ROS 2 into Pandas-compatible storage formats",
    license="BSD",
    entry_points={
        "console_scripts": [
            "rosbag2pandas = rosbag2pandas.rosbag2pandas:main",
        ],
    },
)
