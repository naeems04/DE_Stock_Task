#run.sh
#executes our pipeline code with all the required arguments

python -m pipeline.pipeline \
  --project stockstreamingtask   \
  --runner DataflowRunner \
  --staging_location gs://stock1demo/staging \
  --temp_location gs://stock1demo/tmp \
  --experiments=allow_non_updatable_job parameter \
  --streaming \
  --input_mode stream \
  --input_topic projects/stockstreamingtask/topics/stockdata \
  --output_table stockstreamingtask:dataset1.stock \
  --region europe-west1