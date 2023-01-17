cp src/dist/testcase1/input-0.dat INPUT
cat src/dist/testcase1/input-1.dat >> INPUT
cat src/dist/testcase1/input-2.dat >> INPUT
cat src/dist/testcase1/input-3.dat >> INPUT
cp src/dist/testcase1/output-0.dat OUTPUT
cat src/dist/testcase1/output-1.dat >> OUTPUT
cat src/dist/testcase1/output-2.dat >> OUTPUT
cat src/dist/testcase1/output-3.dat >> OUTPUT
src/bin/showsort INPUT | sort > REF_OUTPUT
diff REF_OUTPUT OUTPUT
