infile=$(readlink -f $1)
shift
cd ~/code/util/runner
python3 sql_runner.py $infile $@
