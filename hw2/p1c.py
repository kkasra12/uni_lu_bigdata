import gc
import os
import time
import fire


def main(class_name, input_folder, output_folder_prefix="output"):
    """
    in this code we will use more folders and calculate the execution time of the code to find the scalability of the code
    to run this code for example you can use the following command
    python p1c.py HadoopWordCount_p1b1 report_p1b1.csv ~/data/wikipedia/enwiki-articles/
    """
    print("Compiling the java code")
    if os.system("javac -classpath $(hadoop classpath) *.java") != 0:
        print("Error in compiling the code")
        return
    print("Code compiled successfully")
    print("Creating the jar file")
    if os.system("jar cf hw2.jar *.class") != 0:
        print("Error in creating the jar file")
        return
    print("Jar file created successfully")
    print("Cleaning the java mess")
    if os.system("rm -rf *.class") != 0:
        print("Error in cleaning the java mess")
        return
    print("Java mess cleaned successfully")
    print("removing output folder")
    output_folder_prefix = f"{output_folder_prefix}_{class_name}"
    print("remove the output folder(s)")
    if (
        os.system(f"rm -rf {output_folder_prefix}*") != 0
    ):  # the code has security issues here!
        print("Error in removing the output folder")
        return
    print("Output folder(s) removed successfully")

    output = [["index", "Number of files", "Execution time"]]
    input_folders = []
    for index, folder_name in enumerate(sorted(os.listdir(input_folder))):
        gc.collect()
        if not os.path.isdir(os.path.join(input_folder, folder_name)):
            continue
        input_folders.append(folder_name)
        start = time.time()
        command = f"hadoop jar hw2.jar {class_name} {input_folder}/{'{'}{','.join(input_folders)}{'}'} {output_folder_prefix}_{folder_name} 2> output_{class_name}_{folder_name}.txt"
        print(f"Running the command: {command}")
        out = os.system(command)
        assert out == 0, f"Error in running the command: {command}, out: {out}"
        output.append([index, ",".join(input_folders), time.time() - start])
        print(output[-1])
    with open(f"report_{class_name}.csv", "w") as f:
        for line in output:
            f.write(",".join(map(str, line)) + "\n")
    print("Report generated successfully")


if __name__ == "__main__":
    """
    for instance use:
    python3 p1c.py HadoopWordCount_p1b1 ~/data/wikipedia/enwiki-articles/
    """
    fire.Fire(main)
