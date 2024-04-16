# in this code we will use more folders and calculate the execution time of the code to find the scalability of the code
# to run this code for example you can use the following command
# bash p1c.sh HadoopWordCount_p1b1 report_p1b1.csv ~/data/wikipedia/enwiki-articles/
code_to_execute=$1
report_file=$2
input_folder=$3

# create a file to store the report
echo "Number of folders, Execution time" >$report_file
input_files=[]
for i in $(ls $input_folder); do
    input_files+=($i)
    echo "Processing $i with input file $input_files"
done
