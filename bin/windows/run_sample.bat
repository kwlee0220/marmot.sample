echo
echo "Running $1..."
java -cp $MARMOT_CLIENT_HOME/bin/marmot.client.jar:$MARMOT_SAMPLE_HOME/build/libs/marmot.sample-1.2.jar	\
$1 $2 $3 $4 $5
