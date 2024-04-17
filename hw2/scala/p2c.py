import gc
import os
import time

import fire


def main(class_name, input_folder, output_folder_prefix="output"):
    if os.system("sbt package") != 0:
        print("Error in creating the jar file")
        return

    report_name = f"report_{class_name}.csv"
    if os.path.exists(report_name):
        with open(report_name, "r") as f:
            last_index = max(
                [int(line.strip().split(",")[0]) for line in f.readlines()]
            )
    else:
        with open(report_name, "w") as f:
            f.write("index,folders,Execution time\n")
        last_index = -1

    input_folders = []
    for index, folder_name in enumerate(sorted(os.listdir(input_folder))):
        gc.collect()
        if not os.path.isdir(os.path.join(input_folder, folder_name)):
            print(f"Skipping {folder_name}")
            continue
        if index <= last_index:
            continue
        input_folders.append(folder_name)

        command = (
            f"spark-submit --class {class_name} "
            "target/scala-2.12/hw2_2.12-1.0.jar "
            f"{input_folder}/{'{'}{','.join(input_folders)}{'}'} "
            f"{output_folder_prefix}_{folder_name} "
            f"2> output_{class_name}_{folder_name}.txt"
        )
        print(f"Running the command: {command}")
        start = time.time()
        out = os.system(command)
        assert out == 0, f"Error in running the command: {command}, out: {out}"
        line = f"{index},{'-'.join(input_folders)},{time.time() - start}\n"
        with open(report_name, "a") as f:
            f.write(line)
        print(line)
    print("Report generated successfully")


if __name__ == "__main__":
    fire.Fire(main)
