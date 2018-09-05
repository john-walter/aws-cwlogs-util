# aws-cwlogs-util

A simple utility meant for "live tailing" and/or viewing CloudWatch logs produced during a given time range.

### Features
* View logs produced during a given time range or leave "--end-time" undefined to view events
* Target a subset of log streams using a regex pattern
* Allows you to continuously refresh log streams during runtime

### Examples

```
// Tail logs in log group "foo", in us-west-2, only targeting stream names containing the string "bar"
// .. Use a credential profile with the name "myprofile" for auth

cwl --log-group-name foo --log-stream-like "bar" --region us-west-2 --profile myprofile

// View logs produced between July and August for log group "foo", in us-east-1, targeting all log streams
// .. Try to use environment variables or EC2 instance profile for auth

cwl --log-group-name foo --start-time 2018-07-01T00:00:00Z --end-time 2018-08-01T00:00:00Z

```
