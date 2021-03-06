[![Build Status](https://travis-ci.org/SuperEvenSteven/environmental-analyzer.svg?branch=master)](https://travis-ci.org/SuperEvenSteven/environmental-analyzer) [![MIT Licence](https://badges.frapsoft.com/os/mit/mit.svg?v=103)](https://opensource.org/licenses/mit-license.php) [![Documentation](https://img.shields.io/badge/code-documented-brightgreen.svg)](https://superevensteven.github.io/environmental-analyzer/index.html)

# environmental-analyzer
Uses Apache Beam to create a distributed data pipeline that combines NexRAD and GSOD datasets to CSV output.

## Data Sets Schemas
- [GSOD Weather Data](https://bigquery.cloud.google.com/dataset/bigquery-public-data:noaa_gsod)
- [GSOD Weather Station](https://bigquery.cloud.google.com/table/bigquery-public-data:noaa_gsod.stations?tab=schema)
- [NexRAD Level II](https://cloud.google.com/storage/docs/public-datasets/nexrad)

## Samples adapted from:
- [Google Cloud BigQueryTornado Example](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/cookbook/BigQueryTornadoes.java)
- [Google Cloud Join Example](https://github.com/GoogleCloudPlatform/DataflowSDK-examples/blob/master-1.x/src/main/java/com/google/cloud/dataflow/examples/cookbook/JoinExamples.java)
- [Valliappa Lakshmanan - Nexrad Pipeline Example](https://github.com/GoogleCloudPlatform/training-data-analyst/blob/master/blogs/nexrad2/src/com/google/cloud/public_datasets/nexrad2/APPipeline.java)

## Useful links
- [Google Cloud Platform - Dataflow SDK Examples](https://github.com/GoogleCloudPlatform/DataflowSDK-examples/tree/master)
- [Valliappa Lakshmanan - Data Science Examples](https://github.com/GoogleCloudPlatform/data-science-on-gcp)

## License
For all files in this repository that don't contain explicit licensing, the MIT license then applies.
See the accompanying LICENSE for more details.

## Visualized Batch Job
Here's an example run using google cloud platform.

![gcp_batch_job.png](gcp_batch_job.png)

