package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/aws/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
)

func checkCredentials(cfg aws.Config) error {
	var err error
	if cfg.Credentials != nil {
		_, err = cfg.Credentials.Retrieve()
	}
	return err
}

func getLogStreams(logger *log.Logger, logsClient *cloudwatchlogs.CloudWatchLogs, logGroupName string, logStreamMatcher *regexp.Regexp, initialStartTime int64) ([]string, error) {
	var logStreamNames []string
	logStreamMinTimestamp := initialStartTime - int64(3*60*60*1000) // 3 hour
	var trueRef = true
	describeLogStreamsInput := cloudwatchlogs.DescribeLogStreamsInput{LogGroupName: &logGroupName, OrderBy: cloudwatchlogs.OrderByLastEventTime, Descending: &trueRef}
	describeLogStreamsRequest := logsClient.DescribeLogStreamsRequest(&describeLogStreamsInput)
	describeLogStreamsRequestIter := describeLogStreamsRequest.Paginate()

	logger.Print("Fetching new set of streams")
	// keep trying to receive log stream names until we complete a pagination cycle with no errors
	matched := map[string]bool{}

loop:
	for {
		found := false
		for describeLogStreamsRequestIter.Next() {
			fmt.Printf(".")
			for _, item := range describeLogStreamsRequestIter.CurrentPage().LogStreams {
				if logStreamMinTimestamp <= *item.LastEventTimestamp {
					logStreamMatches := logStreamMatcher.FindAllStringIndex(*item.LogStreamName, -1)
					if 0 < len(logStreamMatches) && !matched[*item.LogStreamName] {
						logStreamNames = append(logStreamNames, *item.LogStreamName)
						matched[*item.LogStreamName] = true
						found = true
					}
				}
				if logStreamMinTimestamp > *item.LastEventTimestamp {
					break loop
				}
			}
			if describeLogStreamsRequestIter.Err() != nil {
				logger.Printf("Error getting streams: %s", describeLogStreamsRequestIter.Err().Error())
				break
			}
		}
		if !found && describeLogStreamsRequestIter.Err() == nil {
			break
		}
	}
	logger.Print()
	for _, name := range logStreamNames {
		logger.Println(name)
	}
	return logStreamNames, nil
}

type msg []cloudwatchlogs.FilteredLogEvent

func (m msg) Len() int           { return len(m) }
func (m msg) Swap(i, j int)      { m[i], m[j] = m[j], m[i] }
func (m msg) Less(i, j int) bool { return *m[i].IngestionTime < *m[j].IngestionTime }

func main() {

	var logger = log.New(os.Stderr, "", 0)

	profileInput := flag.String("profile", "", "An AWS credential profile (refer to https://docs.aws.amazon.com/cli/latest/userguide/cli-multiple-profiles.html)")
	regionInput := flag.String("region", "us-east-1", "The AWS region associated with the target log group")
	logGroupNameInput := flag.String("log-group-name", "", "An AWS log group that may or may not exist at runtime (polling will continue to occur)")

	logStreamLikeInput := flag.String("log-stream-like", "*", "Target log stream names that match this expression")
	logStreamRefreshInput := flag.Bool("log-stream-refresh", false, "Perform refreshes of target log streams")
	filterPatternInput := flag.String("filter-pattern", "", "A valid CloudWatch log filter (refer to https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/FilterAndPatternSyntax.html)")

	startTimeInput := flag.String("start-time", time.Now().UTC().Format("2006-01-02T15:04:05Z"), "Events that occurred after this time are returned")
	endTimeInput := flag.String("end-time", "", "Events that occurred at or before this time are returned")

	helpInput := flag.Bool("help", false, "Show usage message")
	showInput := flag.Bool("show", false, "Show input data")
	flag.Parse()

	if *showInput {
		logger.Printf("Profile: %s\tRegion: %s\tLog Group: %s\n", *profileInput, *regionInput, *logGroupNameInput)
		logger.Printf("Stream Like: %s\tPattern: %s\n", *logStreamLikeInput, *filterPatternInput)
		logger.Printf("Start: [%s], End: [%s]\n", *startTimeInput, *endTimeInput)
	}

	if *logGroupNameInput == "" || *logStreamLikeInput == "" || *helpInput {
		flag.Usage()
		logger.Fatal("Stopping")
	}

	cfg, err := external.LoadDefaultAWSConfig(
		external.WithMFATokenFunc(stscreds.StdinTokenProvider),
		external.WithSharedConfigProfile(*profileInput),
		external.WithRegion(*regionInput),
	)
	if err != nil {
		logger.Fatalf("Failed LoadDefaultAWSConfig: %s", err.Error())
	}

	if err = checkCredentials(cfg); err != nil {
		logger.Fatalf(`Ensure that your credential profile %s has been properly configured (refer to https://docs.aws.amazon.com/cli/latest/userguide/cli-multiple-profiles.html).`, *profileInput)
	}

	logsClient := cloudwatchlogs.New(cfg)

	logStreamMatcher := regexp.MustCompile(strings.Replace(*logStreamLikeInput, "*", ".*", -1))

	stscreds.DefaultDuration = time.Minute * 60

	var startTime time.Time // From parameters
	var endTime time.Time   // From parameters

	var startTimeUnix int64 // milliseconds, working value
	var endTimeUnix int64   // milliseconds, working value

	startTime = toTime(*startTimeInput, time.Now())

	endTime = toTime(*endTimeInput, startTime.Add(24*time.Hour))

	startTimeUnix = startTime.UTC().UnixNano() / (1000 * 1000)
	endTimeUnix = endTime.UTC().UnixNano() / (1000 * 1000)

	if *showInput {
		fmt.Printf("Using:  Start: %s; End: %s\n", startTime.Format(time.RFC3339Nano), endTime.Format(time.RFC3339Nano))
		s := time.Unix(0, startTimeUnix*(1000*1000))
		e := time.Unix(0, endTimeUnix*(1000*1000))
		fmt.Printf("Within: Start: %s; End: %s\n\n", s.Format(time.RFC3339Nano), e.Format(time.RFC3339Nano))
	}

	filterLogEventsInput := cloudwatchlogs.FilterLogEventsInput{LogGroupName: logGroupNameInput, StartTime: &startTimeUnix, EndTime: &endTimeUnix}
	if len(*filterPatternInput) > 0 {
		filterLogEventsInput.FilterPattern = filterPatternInput
	}

	c := make(chan os.Signal, 100)
	signal.Notify(c, syscall.SIGUSR1)

	start := time.Time{}
	for {

		select {
		case sig := <-c:
			if sig == syscall.SIGUSR1 {
				logger.Println("Restarting")
				start = time.Time{}
			}
		default:
		}

		if time.Since(start) > (5 * time.Minute) {
			start = time.Now()
			filterLogEventsInput.LogStreamNames, err = getLogStreams(logger, logsClient, *logGroupNameInput, logStreamMatcher, startTimeUnix)
			if err != nil {
				time.Sleep(5 * time.Second)
				continue
			}
		}
		filterLogEventsRequest := logsClient.FilterLogEventsRequest(&filterLogEventsInput)
		filterLogEventsRequestIter := filterLogEventsRequest.Paginate()

		all := make(chan cloudwatchlogs.FilteredLogEvent)
		go func(filterLogEventsRequestIter *cloudwatchlogs.FilterLogEventsPager) {
			defer close(all)
			for filterLogEventsRequestIter.Next() {
				filterLogEventsOutput := *filterLogEventsRequestIter.CurrentPage()
				for _, item := range filterLogEventsOutput.Events {
					all <- item
				}
			}
		}(&filterLogEventsRequestIter)

		var latest int64
		var output []cloudwatchlogs.FilteredLogEvent
		for row := range all {
			if *row.Timestamp > latest {
				latest = *row.Timestamp
			}
			output = append(output, row)
		}

		sort.Sort(msg(output))
		for _, o := range output {
			p := strings.Split(*o.LogStreamName, "/")
			logger.Printf("%-30s  %s\n", strings.Join(p[:1], "/"), *o.Message)
		}

		err = filterLogEventsRequestIter.Err()
		if err != nil {
			logger.Printf("Error paging: %s", err.Error())
			continue
		}

		if !endTime.IsZero() && (endTime.UnixNano()/(1000*1000)) <= latest {
			return
		}

		if len(output) == 0 {
			time.Sleep(5 * time.Second)
			continue
		}
		startTimeUnix = latest + 1

		filterLogEventsInput.StartTime = &startTimeUnix

		if !*logStreamRefreshInput {
			return
		}

		filterLogEventsInput.EndTime = &endTimeUnix
	}
}
func toTime(in string, def time.Time) time.Time {
	t, err := time.Parse(time.RFC3339, in)
	if err != nil {
		t, err = time.Parse("2006-01-02 15:04:05.999", in)
	}
	if err != nil {
		t = def
	}
	return t
}
