[![Build Status](https://travis-ci.org/SuperEvenSteven/environmental-analyzer.svg?branch=master)](https://travis-ci.org/SuperEvenSteven/environmental-analyzer) [![MIT Licence](https://badges.frapsoft.com/os/mit/mit.svg?v=103)](https://opensource.org/licenses/mit-license.php)

# nexrad-decode-sample
An example of decoding publicly available NEXRAD (Level II) data using Java 8 and NetCDF4. 

Sample adapted from:
* https://www1.ncdc.noaa.gov/pub/data/radar/Radar-Decoding-JavaSolution.txt

## Changes: 
- Updated this to use the unidata dependencies straight from their personal public repo 
- Refactored the use of deprecated unidata classes
- Created a standard maven project structure 
- Implemented a simple CI build script using Travis CI

## Useful links
- [AWS Public Datasets](https://aws.amazon.com/public-datasets/nexrad/)
- [Unidata Nexus Maven Repo](https://artifacts.unidata.ucar.edu/)

# environmental-analyzer
Uses Apache Beam to create a distributed data pipeline that combines NexRAD and GSOD datasets to CSV output.
