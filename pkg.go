package main

import (
	"log"
	//
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"

	"github.com/jessevdk/go-flags"

	"fmt"
	"os"
)

//var MAGIC = []byte("\xf3\x89\x9a\xc2")

type Options struct {
	Region string `long:"region" description:"AWS Region" required:"true" env:"AWS_REGION"`

	Args struct {
		StreamName string `positional-arg-name:"STREAM_NAME" required:"true"`
	} `positional-args:"yes" required:"yes"`
}

var exitFn = os.Exit

func fatalfIfErr(format string, err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, format, err)
		exitFn(1)
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

func main() {
	var opts Options
	_, err := flags.ParseArgs(&opts, os.Args[1:])
	if err != nil {
		exitFn(2)
	}

	var client = buildKinesisClient(opts.Region)
	var shardIds = getShardIds(client, opts.Args.StreamName)
	log.Print(shardIds)

	//ctx, cancel := context.WithCancel(ctx)
	//defer cancel()
	//
	//
	//
	//
	//
	//// start
	//err = c.Scan(context.TODO(), func(r *consumer.Record) consumer.ScanError {
	//	if r.ApproximateArrivalTimestamp.Before(startTimestamp) {
	//		return consumer.ScanError{
	//			StopScan:       false,  // true to stop scan
	//			SkipCheckpoint: false,  // true to skip checkpoint
	//		}
	//	}
	//
	//	agg := &kpl.AggregatedRecord{}
	//
	//	data := r.Data
	//
	//	// see https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md
	//	is_aggregated := bytes.Compare(data[0:len(MAGIC)], MAGIC) == 0
	//
	//	if is_aggregated {
	//
	//		err := proto.Unmarshal(data[4:len(data)-15], agg)
	//		if err != nil {
	//			// ignore errors
	//			// log.Printf("Failed to parse record:", err)
	//		}
	//
	//		for _, r := range agg.Records {
	//			fmt.Println(string(r.Data))
	//		}
	//	} else {
	//		fmt.Println(string(data))
	//	}
	//
	//	// continue scanning
	//	return consumer.ScanError{
	//		StopScan:       false,  // true to stop scan
	//		SkipCheckpoint: false,  // true to skip checkpoint
	//	}
	//})
	//if err != nil {
	//	log.Fatalf("scan error: %v", err)
	//}

	// Note: If you need to aggregate based on a specific shard the `ScanShard`
	// method should be leverged instead.
}
