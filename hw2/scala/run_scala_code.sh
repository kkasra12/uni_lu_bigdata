class_name="Q2b4"
sbt package
read -n 1 -s -r -p "Press any key to continue, or press Ctrl+C to exit..."
out_dir="output_$class_name"
rm -rf $out_dir
spark-submit --class $class_name target/scala-2.12/hw2_2.12-1.0.jar ~/data/wikipedia/enwiki-articles/AA $out_dir
cat $out_dir/part-* >$out_dir/output.txt
echo "Output is saved in $out_dir/output.txt"
echo "Outputs summary:"
head $out_dir/output.txt
echo -e ".\n.\n.\n"
tail $out_dir/output.txt
