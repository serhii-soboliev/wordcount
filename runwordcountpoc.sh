export PROJECT=oreilly-labs
export BUCKET=dataflow-input-bucket
export REGION=us-central1
python3 wc/wordcountpoc.py \
--region $REGION \
--input gs://dataflow-samples/shakespeare/kinglear.txt \
--output gs://$BUCKET/results/outputs \
--runner DataflowRunner \
--project $PROJECT \
--temp_location gs://$BUCKET/tmp/ \
--job_name wordcountjob1
