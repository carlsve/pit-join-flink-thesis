run_view:
	./run_java.sh VIEW PARTITIONED

run:
	./run_java.sh

runn:
	mvn exec:java -Dexec.mainClass="test.Main" -Dexec.args="$JAVA_PROGRAM_ARGS"

data_gen_tests:
	python datagenerator/generate_all_variants.py 10000 20 2 ./datasets

test_data_gen:
	python datagenerator/generate_all_variants.py 10000 20 2 ./datasets
	python datagenerator/generate_all_variants.py 10000 80 8 ./datasets
	python datagenerator/generate_all_variants.py 100000 20 2 ./datasets
	python datagenerator/generate_all_variants.py 100000 80 8 ./datasets

	tree -L 4 datasets

bigger_data_gen:
	python datagenerator/generate_all_variants.py 1000000 20 2 ./datasets

biggest_data_gen:
	python datagenerator/generate_all_variants.py 1000000 80 8 ./datasets

gen_ten_thou:
	python datagenerator/generate_all_variants.py 10000 20 2 ./datasets
	python datagenerator/generate_all_variants.py 10000 80 8 ./datasets

gen_hun_thou:
	python datagenerator/generate_all_variants.py 100000 20 2 ./datasets
	python datagenerator/generate_all_variants.py 100000 80 8 ./datasets

gen_one_mil:
	python datagenerator/generate_all_variants.py 1000000 20 2 ./datasets
	python datagenerator/generate_all_variants.py 1000000 80 8 ./datasets

gen_all:
	python datagenerator/generate_all_variants.py 10000 20 2 ./datasets
	python datagenerator/generate_all_variants.py 10000 80 8 ./datasets

	python datagenerator/generate_all_variants.py 25000 20 2 ./datasets
	python datagenerator/generate_all_variants.py 25000 80 8 ./datasets

	python datagenerator/generate_all_variants.py 50000 20 2 ./datasets
	python datagenerator/generate_all_variants.py 50000 80 8 ./datasets

	python datagenerator/generate_all_variants.py 75000 20 2 ./datasets
	python datagenerator/generate_all_variants.py 75000 80 8 ./datasets

	python datagenerator/generate_all_variants.py 100000 20 2 ./datasets
	python datagenerator/generate_all_variants.py 100000 80 8 ./datasets
