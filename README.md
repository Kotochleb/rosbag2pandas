# rosbag2pandas
CLI tool used to convert ROS 2 bag files to Pandas-compatible storage formats.

Usage
```bash
ros2 run rosbag2pandas <input bag folder> <output folder> --format csv
```

For following list of topics:
```bash
/turtle1/cmd_vel
/turtle1/color_sensor
/turtle1/pose
```
it will create following files:
```
output_folder/
├─ turtle1_cmd_vel.csv
├─ turtle1_color_sensor.csv
├─ turtle1_pose.csv
```

## Output formats

Currently supported formats are:
- csv
- parquet
- pickle
