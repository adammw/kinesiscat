package main

import (
	"bytes"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/golang/protobuf/proto"
	"github.com/jessevdk/go-flags"
	"github.com/zendesk/kinesiscat/kpl"
	"sync"
	"time"
)

const Md5Len = 16

var ProtobufHeader = []byte("\xf3\x89\x9a\xc2")
var Newline = []byte{'\n'}

type Options struct {
	Region string `long:"region" description:"AWS Region" required:"true" env:"AWS_REGION"`

	Args struct {
		StreamName string `positional-arg-name:"STREAM_NAME" required:"true"`
	} `positional-args:"yes" required:"yes"`
}

var exitFn = os.Exit

func fatalErr(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format, a...)
	exitFn(1)
}

func fatalfIfErr(format string, err error) {
	if err != nil {
		fatalErr(format, err)
	}
}

func buildKinesisClient(region string) (client kinesisiface.KinesisAPI) {
	awsConfig := aws.NewConfig().WithRegion(region)
	awsSession, err := session.NewSession(awsConfig)
	fatalfIfErr("aws error: %v", err)
	return kinesis.New(awsSession, awsConfig)
}

func getShardIds(client kinesisiface.KinesisAPI, streamName string) (shardIds []string) {
	shardIds = []string{}
	params := kinesis.DescribeStreamInput{
		StreamName: aws.String(streamName),
	}
	err := client.DescribeStreamPages(
		&params,
		func(page *kinesis.DescribeStreamOutput, lastPage bool) bool {
			for _, shard := range page.StreamDescription.Shards {
				shardIds = append(shardIds, *shard.ShardId)
			}
			return true
		},
	)
	fatalfIfErr("get shards error: %v", err)
	return
}

func getShardIterators(client kinesisiface.KinesisAPI, streamName string, shardIds []string) (shardIterators []string) {
	shardIterators = []string{}
	shardIteratorType := kinesis.ShardIteratorTypeLatest
	for _, shardId := range shardIds {
		iterator, err := client.GetShardIterator(&kinesis.GetShardIteratorInput{
			ShardId:           &shardId,
			ShardIteratorType: &shardIteratorType,
			StreamName:        &streamName,
		})
		fatalfIfErr("get iterator error: %v", err)
		shardIterators = append(shardIterators, *iterator.ShardIterator)
	}
	return
}

func streamRecords(client kinesisiface.KinesisAPI, shardIterator string, fn func(*[]byte)) {
	var err error
	var resp *kinesis.GetRecordsOutput
	var errCount uint = 0

	for err == nil {
		params := kinesis.GetRecordsInput{
			ShardIterator: &shardIterator,
		}

		resp, err = client.GetRecords(&params)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				// retry on ProvisionedThroughputExceededException with exponential backoff
				if aerr.Code() == kinesis.ErrCodeProvisionedThroughputExceededException {
					time.Sleep(1 << errCount)
					errCount += 1
					err = nil
					continue
				} else {
					fatalErr("get records error: %v", err)
				}
			}
		} else {
			// reset backoff counter on success
			errCount = 0
		}

		eachRecord(resp.Records, fn)

		if resp.NextShardIterator == nil {
			break
		}
		shardIterator = *resp.NextShardIterator
	}
}

func eachRecord(aggregatedRecords []*kinesis.Record, fn func(*[]byte)) {
	for _, record := range aggregatedRecords {
		isAggregated := bytes.Compare(record.Data[0:len(ProtobufHeader)], ProtobufHeader) == 0

		if isAggregated {
			// see https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md
			agg := &kpl.AggregatedRecord{}
			err := proto.Unmarshal(record.Data[len(ProtobufHeader):len(record.Data)-Md5Len], agg)
			fatalfIfErr("protobuf unmarshal error: %v", err)
			for _, record := range agg.Records {
				fn(&record.Data)
			}
		} else {
			fn(&record.Data)
		}
	}
}

func parallel(things []string, fn func(string)) {
	var wg sync.WaitGroup
	for _, thing := range things {
		wg.Add(1)
		go func(thing string) {
			defer wg.Done()
			fn(thing)
		}(thing)
	}
	wg.Wait()
}

func main() {
	var opts Options
	_, err := flags.ParseArgs(&opts, os.Args[1:])
	if err != nil {
		exitFn(2)
	}

	var client = buildKinesisClient(opts.Region)
	var shardIds = getShardIds(client, opts.Args.StreamName)
	var shardIterators = getShardIterators(client, opts.Args.StreamName, shardIds)

	parallel(shardIterators, func(shardIterator string) {
		streamRecords(client, shardIterator, func(data *[]byte) {
			line := append([]byte{}, *data...)
			line = append(line, Newline...)
			os.Stdout.Write(line)
		})
	})
}
