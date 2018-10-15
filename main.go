package main

import (
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/aws/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"log"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

var logsClient = cloudwatchlogs.CloudWatchLogs{}
var logger = log.New(os.Stdout, "", 0)
var waitGroup = sync.WaitGroup{}
// maintain an artifical time offset (in seconds) so we can view log events ingested in realtime, but might have an earlier event timestamp up to this value in the past
var timeOffset = -int64(60 * 1000)
var trueRef = true

func getCredentials(cfg aws.Config) bool {
	var err error
	if cfg.Credentials != nil {
		_, err = cfg.Credentials.Retrieve()
	}
	return err == nil
}

func getLogStreams(logGroupName string, logStreamMatcher regexp.Regexp, minEventTime int64, maxEventTime int64) []string {
	logStreamNames := []string{}
	logStreamMinTimestamp := minEventTime - (60 * 60 * 1000 * 3)
	logStreamMaxTimestamp := maxEventTime
	describeLogStreamsInput := cloudwatchlogs.DescribeLogStreamsInput{LogGroupName: &logGroupName, OrderBy: cloudwatchlogs.OrderByLastEventTime}
	describeLogStreamsRequest := logsClient.DescribeLogStreamsRequest(&describeLogStreamsInput)
	describeLogStreamsRequestIter := describeLogStreamsRequest.Paginate()
	encounteredLaterTimestamp := false
	// keep trying to receive log stream names until we complete a pagination cycle with no errors
	for !encounteredLaterTimestamp && describeLogStreamsRequestIter.Next() {
		describeLogStreamsRequestPage := describeLogStreamsRequestIter.CurrentPage()
		for _, item := range describeLogStreamsRequestPage.LogStreams {
			if item.LastEventTimestamp == nil { continue }
			
			if logStreamMinTimestamp <= *item.LastEventTimestamp && *item.LastEventTimestamp < logStreamMaxTimestamp {
				logStreamMatches := logStreamMatcher.FindAllStringIndex(*item.LogStreamName, -1)
				if 0 < len(logStreamMatches) {
					logStreamNames = append(logStreamNames, *item.LogStreamName)
				}
			} else if logStreamMaxTimestamp <= *item.LastEventTimestamp {
				encounteredLaterTimestamp = true
			}
		}
		if describeLogStreamsRequestIter.Err() != nil {
			time.Sleep(time.Second * 5)
		} else if describeLogStreamsRequestPage.NextToken == nil {
			break
		}
	}
	return logStreamNames
}

func main() {
	stscreds.DefaultDuration = time.Minute * 60
	profileInput := flag.String("profile", "", "An AWS credential profile (refer to https://docs.aws.amazon.com/cli/latest/userguide/cli-multiple-profiles.html)")
	regionInput := flag.String("region", "us-east-1", "The AWS region associated with the target log group")
	logGroupNameInput := flag.String("log-group-name", "", "An AWS log group that may or may not exist at runtime (polling will continue to occur)")
	logStreamLikeInput := flag.String("log-stream-like", "*", "Target log stream names that match this expression")
	logStreamRefreshInput := flag.Bool("log-stream-refresh", false, "Perform refreshes of target log streams")
	filterPatternInput := flag.String("filter-pattern", "", "A valid CloudWatch log filter (refer to https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/FilterAndPatternSyntax.html)")
	startTimeInput := flag.String("start-time", time.Now().UTC().Format("2006-01-02T15:04:05Z"), "Events that occurred after this time are returned")
	endTimeInput := flag.String("end-time", "", "Events that occurred at or before this time are returned")
	helpInput := flag.Bool("help", false, "Show usage message")
	flag.Parse()
	if *logGroupNameInput == "" || *logStreamLikeInput == "" || *helpInput {
		flag.PrintDefaults()
		os.Exit(1)
	}

	logStreamMatcher := regexp.MustCompile(strings.Replace(*logStreamLikeInput, "*", ".*", -1))

	startTime, _ := time.Parse(time.RFC3339, *startTimeInput)

	cfg, _ := external.LoadDefaultAWSConfig(
		external.WithMFATokenFunc(stscreds.StdinTokenProvider),
		external.WithSharedConfigProfile(*profileInput),
		external.WithRegion(*regionInput),
	)

	if &cfg != nil && getCredentials(cfg) {
		logsClient = *cloudwatchlogs.New(cfg)
	} else {
		additionalProfileHelp := ""
		if *profileInput != "" {
			additionalProfileHelp = " Ensure that your credential profile \"" + *profileInput + "\" has been properly configured (refer to https://docs.aws.amazon.com/cli/latest/userguide/cli-multiple-profiles.html)."
		}
		fmt.Println("Bad credentials were provided." + additionalProfileHelp)
		os.Exit(1)
	}

	startTimeUnix := startTime.UTC().Unix() * 1000
	initialStartTimeUnix := startTimeUnix

	filterLogEventsInput := cloudwatchlogs.FilterLogEventsInput{LogGroupName: logGroupNameInput, StartTime: &startTimeUnix}
	if *filterPatternInput != "" {
		filterLogEventsInput.FilterPattern = filterPatternInput
	}

	var endTimeUnix int64
	if *endTimeInput != "" {
		endTime, _ := time.Parse(time.RFC3339, *endTimeInput)
		endTimeUnix = endTime.UTC().Unix() * 1000
	} else {
		endTimeUnix = time.Now().UTC().Unix() * 1000 + timeOffset
	}
	filterLogEventsInput.EndTime = &endTimeUnix

	var filterLogEventsError error
	logStreamNames := getLogStreams(*logGroupNameInput, *logStreamMatcher, initialStartTimeUnix, endTimeUnix)
	for 0 < len(logStreamNames) || *endTimeInput == "" {
		for i := 0; i < len(logStreamNames); i += 100 {
			endOfRange := i + 99
			if len(logStreamNames) - 1 < endOfRange {
				endOfRange = i + (len(logStreamNames) % 100)
			}
			filterLogEventsInput.LogStreamNames = logStreamNames[i:endOfRange]
			filterLogEventsRequest := logsClient.FilterLogEventsRequest(&filterLogEventsInput)
			filterLogEventsRequestIter := filterLogEventsRequest.Paginate()
			for filterLogEventsRequestIter.Next() {
				waitGroup.Add(1)
				go func(filterLogEventsOutput cloudwatchlogs.FilterLogEventsOutput) {
					defer waitGroup.Done()
					for _, item := range filterLogEventsOutput.Events {
						logger.Print(time.Unix(0, *item.Timestamp * int64(time.Millisecond)).UTC().Format("2006-01-02T15:04:05Z") + "\t" + *item.LogStreamName + "\t" + *item.Message)
					}
				}(*filterLogEventsRequestIter.CurrentPage())
			}
			filterLogEventsError = filterLogEventsRequestIter.Err()
			if filterLogEventsError != nil {
				break
			}
		}
		waitGroup.Wait()
		if filterLogEventsError == nil && *endTimeInput == "" {
			startTimeUnix = endTimeUnix + 1000
			for endTimeUnix <= startTimeUnix {
				time.Sleep(time.Second)
				endTimeUnix = (endTimeUnix * 1000) + timeOffset + 1000
			}
			filterLogEventsInput.StartTime = &startTimeUnix
			filterLogEventsInput.EndTime = &endTimeUnix
			if *logStreamRefreshInput {
				logStreamNames = getLogStreams(*logGroupNameInput, *logStreamMatcher, initialStartTimeUnix, endTimeUnix)
			}
		} else {
			break
		}
	}
}
