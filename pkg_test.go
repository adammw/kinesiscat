package main

import (
	"bytes"
	"io"
	"os"
	"testing"

	"errors"
	"fmt"
	"github.com/adammw/kinesiscat/kpl"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/golang/protobuf/proto"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"strconv"
	"time"
)

func TestSetup(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "kinesiscat")
}

// https://stackoverflow.com/questions/10473800/in-go-how-do-i-capture-stdout-of-a-function-into-a-string
func captureStdout(fn func()) (captured string) {
	old := os.Stdout // keep backup of the real
	r, w, _ := os.Pipe()
	os.Stdout = w

	fn()

	outC := make(chan string)
	// copy the output in a separate goroutine so printing can't block indefinitely
	go func() {
		var buf bytes.Buffer
		io.Copy(&buf, r)
		outC <- buf.String()
	}()

	// back to normal state
	w.Close()
	os.Stdout = old // restoring the real
	captured = <-outC
	return
}

func captureStderr(fn func()) (captured string) {
	old := os.Stderr // keep backup of the real
	r, w, _ := os.Pipe()
	os.Stderr = w

	fn()

	outC := make(chan string)
	// copy the output in a separate goroutine so printing can't block indefinitely
	go func() {
		var buf bytes.Buffer
		io.Copy(&buf, r)
		outC <- buf.String()
	}()

	// back to normal state
	w.Close()
	os.Stderr = old // restoring the real
	captured = <-outC
	return
}

func captureAll(fn func()) (stdout string, stderr string) {
	stdout = captureStdout(func() {
		stderr = captureStderr(fn)
	})
	return
}

func withOsArgs(args []string, fn func()) {
	old := os.Args
	os.Args = args
	defer func() {
		os.Args = old
	}()
	fn()
}

func captureExit(fn func()) (code int) {
	code = -1
	origExitFn := exitFn
	exitFn = func(c int) { code = c; panic(nil) }
	defer func() {
		exitFn = origExitFn
		recover()
	}()
	fn()
	return
}

func captureExitAndMessage(fn func()) (exitCode int, stderr string) {
	stderr = captureStderr(func() {
		exitCode = captureExit(fn)
	})
	return
}

var shards []*kinesis.Shard
var getRecordsOutput = &kinesis.GetRecordsOutput{
	Records:           []*kinesis.Record{},
	NextShardIterator: nil,
}
var getRecordsErr error

type mockKinesisClient struct {
	kinesisiface.KinesisAPI
}

func (m *mockKinesisClient) GetRecords(*kinesis.GetRecordsInput) (output *kinesis.GetRecordsOutput, err error) {
	return getRecordsOutput, getRecordsErr
}

func (m *mockKinesisClient) GetShardIterator(input *kinesis.GetShardIteratorInput) (output *kinesis.GetShardIteratorOutput, err error) {
	iterator := fmt.Sprintf("shard-iterator-%s-%s", *input.StreamName, *input.ShardId)
	output = &kinesis.GetShardIteratorOutput{
		ShardIterator: &iterator,
	}
	return
}

func (m *mockKinesisClient) DescribeStreamPages(input *kinesis.DescribeStreamInput, fn func(*kinesis.DescribeStreamOutput, bool) bool) error {
	output := kinesis.DescribeStreamOutput{
		StreamDescription: &kinesis.StreamDescription{
			Shards: shards,
		},
	}

	fn(&output, true)
	return nil
}

func (m *mockKinesisClient) ListStreams(input *kinesis.ListStreamsInput) (output *kinesis.ListStreamsOutput, err error) {
	name := "stream-a"
	return &kinesis.ListStreamsOutput{StreamNames: []*string{&name}}, nil
}

var callCount = 0

type mockErrKinesisClient struct {
	kinesisiface.KinesisAPI
}

func (m *mockErrKinesisClient) GetRecords(*kinesis.GetRecordsInput) (output *kinesis.GetRecordsOutput, err error) {
	callCount += 1
	if callCount == 1 {
		return nil, getRecordsErr
	} else {
		return getRecordsOutput, nil
	}

}

func duration(fn func()) (duration time.Duration) {
	start := time.Now()
	fn()
	return time.Since(start)
}

func withFakeKinesisClient(fn func()) {
	old := buildKinesisClient
	buildKinesisClient = func(region string) (client kinesisiface.KinesisAPI) {
		return &mockKinesisClient{}
	}
	defer func() { buildKinesisClient = old }()
	fn()
}

var _ = Describe("kinesiscat", func() {
	BeforeEach(func() {
		getRecordsOutput.Records = []*kinesis.Record{}
		getRecordsOutput.NextShardIterator = nil
		getRecordsErr = nil
		shards = []*kinesis.Shard{}
		ExponentialBackoffBase = time.Millisecond
		callCount = 0
	})

	Describe("main", func() {
		It("shows --help", func() {
			withOsArgs([]string{"kinesiscat", "--help"}, func() {
				out := captureStdout(func() {
					Expect(captureExit(main)).To(Equal(2))
				})
				Expect(out).To(ContainSubstring("Usage:"))
			})
		})

		It("shows available streams when user did not chose one", func() {
			withFakeKinesisClient(func() {
				withOsArgs([]string{"kinesiscat", "--region", "us-west-1"}, func() {
					code, out := captureExitAndMessage(main)
					Expect(out).To(Equal("No stream name give, please chose one of these streams:\nstream-a\n"))
					Expect(code).To(Equal(2))
				})
			})
		})

		It("streams messages", func() {
			withFakeKinesisClient(func() {
				shardId := "123"
				shards = []*kinesis.Shard{{ShardId: &shardId}}
				getRecordsOutput.Records = []*kinesis.Record{{Data: []byte{'1', '2'}}}

				stdout := captureStdout(func() {
					withOsArgs([]string{"kinesiscat", "my-stream", "--region", "us-west-1"}, main)
				})
				Expect(stdout).To(Equal("12\n"))
			})
		})
	})

	Describe(".fatalfIfErr", func() {
		It("no-ops when err == nil", func() {
			exitCode := captureExit(func() {
				fatalfIfErr("foo", nil)
			})
			Expect(exitCode).To(Equal(-1)) // not called
		})

		It("exits when there is an err", func() {
			err := errors.New("bar")
			exitCode, stderr := captureExitAndMessage(func() {
				fatalfIfErr("foo: %v", err)
			})
			Expect(exitCode).To(Equal(1))
			Expect(stderr).To(Equal("foo: bar"))
		})
	})

	Describe(".buildKinesisClient", func() {
		It("builds a client", func() {
			client := buildKinesisClient("us-west-1")
			Expect(client).To(Not(BeNil()))
		})
	})

	Describe(".eachRecord", func() {
		It("passes through un-aggregated records", func() {
			inRecords := []*kinesis.Record{
				{Data: []byte("foo")},
				{Data: []byte("bar")},
			}
			outRecords := [][]byte{}
			eachRecord(inRecords, func(record *[]byte) {
				outRecords = append(outRecords, *record)
			})
			Expect(outRecords).To(ContainElement([]byte("foo")))
			Expect(outRecords).To(ContainElement([]byte("bar")))
		})

		It("handles aggregated records", func() {
			partitionKeyIndex := uint64(0)
			protoData, err := proto.Marshal(&kpl.AggregatedRecord{
				Records: []*kpl.Record{
					{PartitionKeyIndex: &partitionKeyIndex, Data: []byte("foo")},
					{PartitionKeyIndex: &partitionKeyIndex, Data: []byte("bar")},
				},
			})
			Expect(err).To(BeNil())
			md5Data := make([]byte, Md5Len) // TODO: calculate checksum

			aggregatedRecordData := ProtobufHeader
			aggregatedRecordData = append(aggregatedRecordData, protoData...)
			aggregatedRecordData = append(aggregatedRecordData, md5Data...)
			inRecords := []*kinesis.Record{{Data: aggregatedRecordData}}

			outRecords := [][]byte{}
			eachRecord(inRecords, func(record *[]byte) {
				outRecords = append(outRecords, *record)
			})
			Expect(outRecords).To(ContainElement([]byte("foo")))
			Expect(outRecords).To(ContainElement([]byte("bar")))
		})
	})

	Describe(".getShardIds", func() {
		It("returns empty array when no shards found", func() {
			mockSvc := &mockKinesisClient{}
			actual := getShardIds(mockSvc, "fake-stream")
			Expect(actual).To(Equal([]string{}))
		})

		It("returns an array of shards found", func() {
			mockSvc := &mockKinesisClient{}
			shardIds := []string{"foo123", "foo124"}
			shards = []*kinesis.Shard{{ShardId: &shardIds[0]}, {ShardId: &shardIds[1]}}

			actual := getShardIds(mockSvc, "fake-stream")
			Expect(actual).To(Equal(shardIds))
		})
	})

	Describe(".getShardIterators", func() {
		It("returns an array of shard iterator ids", func() {
			mockSvc := &mockKinesisClient{}
			shardIds := []string{"foo123", "foo124"}
			shardIterators := []string{"shard-iterator-fake-stream-foo123", "shard-iterator-fake-stream-foo124"}
			actual := getShardIterators(mockSvc, "fake-stream", shardIds)
			Expect(actual).To(Equal(shardIterators))
		})
	})

	Describe(".parallel", func() {
		It("runs the method with the arg passed in for each", func() {
			things := []string{"foo", "bar"}
			calls := []string{}
			parallel(things, func(thing string) {
				calls = append(calls, thing)
			})
			Expect(len(calls)).To(Equal(len(things)))
			Expect(calls).To(ContainElement("foo"))
			Expect(calls).To(ContainElement("bar"))
		})

		It("runs the methods in parallel", func() {
			sleeps := []string{"10", "20", "30"}
			elapsed := duration(func() {
				parallel(sleeps, func(thing string) {
					ms, _ := strconv.ParseInt(thing, 10, 8)
					time.Sleep(time.Duration(ms) * time.Millisecond)
				})
			})
			Expect(elapsed).To(BeNumerically("~", 35*time.Millisecond, 5*time.Millisecond))
		})
	})

	Describe(".streamRecords", func() {
		It("streams regular records", func() {
			getRecordsOutput.Records = []*kinesis.Record{{Data: []byte{'1'}}, {Data: []byte{'2'}}}
			calls := []byte{}
			streamRecords(&mockKinesisClient{}, "AABBCC", func(data *[]byte) {
				calls = append(calls, *data...)
			})
			Expect(calls).To(Equal([]byte{'1', '2'}))
		})

		It("iterates until the iterator is gone", func() {
			getRecordsOutput.Records = []*kinesis.Record{{Data: []byte{'1'}}, {Data: []byte{'2'}}}
			nextIterator := "next"
			getRecordsOutput.NextShardIterator = &nextIterator

			calls := []byte{}
			streamRecords(&mockKinesisClient{}, "first", func(data *[]byte) {
				if len(calls) == 2 {
					getRecordsOutput.NextShardIterator = nil
				}
				calls = append(calls, *data...)
			})
			Expect(calls).To(Equal([]byte{'1', '2', '1', '2'}))
		})

		It("expands protobuf", func() {
			// given an AggregatedRecord
			var index uint64 = 1
			aggregatedRecord := &kpl.AggregatedRecord{
				Records: []*kpl.Record{
					{Data: []byte{'1'}, PartitionKeyIndex: &index},
					{Data: []byte{'2'}, PartitionKeyIndex: &index},
				}}

			// marshal and add header + fake md5
			aggregate, err := proto.Marshal(aggregatedRecord)
			fatalfIfErr("Marshal", err)
			aggregate = append(ProtobufHeader, aggregate...)
			aggregate = append(aggregate, []byte{'1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '1', '2', '3', '4', '5', '6'}...)

			// stream aggregate and a regular record
			getRecordsOutput.Records = []*kinesis.Record{{Data: aggregate}, {Data: []byte{'3'}}}
			calls := []byte{}
			streamRecords(&mockKinesisClient{}, "AABBCC", func(data *[]byte) {
				calls = append(calls, *data...)
			})

			Expect(calls).To(Equal([]byte{'1', '2', '3'}))
		})

		It("fails when hitting unknown error", func() {
			getRecordsErr = errors.New("Whoops")
			exitCode, stderr := captureExitAndMessage(func() {
				streamRecords(&mockErrKinesisClient{}, "AABBCC", func(data *[]byte) {
					Fail("Should not go here")
				})
			})
			Expect(exitCode).To(Equal(1))
			Expect(stderr).To(Equal("get records error: Whoops"))
		})

		It("retries when hitting ThroughputExceededException", func() {
			getRecordsOutput.Records = []*kinesis.Record{{Data: []byte{'1'}}, {Data: []byte{'2'}}}
			getRecordsErr = awserr.New(kinesis.ErrCodeProvisionedThroughputExceededException, "Throttled", nil)
			calls := []byte{}
			elapsed := duration(func() {
				out := captureStderr(func() {
					streamRecords(&mockErrKinesisClient{}, "AABBCC", func(data *[]byte) {
						calls = append(calls, *data...)
					})
				})
				Expect(out).To(Equal("Throughput limit exceeded, waiting 1ms\n"))

			})
			Expect(calls).To(Equal([]byte{'1', '2'}))
			Expect(elapsed).To(BeNumerically("~", 2*time.Millisecond, time.Millisecond))
			Expect(ExponentialBackoffBase).To(Equal(time.Millisecond)) // not modified
		})

		It("retries when hitting unknown aws error", func() {
			getRecordsErr = awserr.New("Code", "Throttled", nil)
			exitCode, stderr := captureExitAndMessage(func() {
				streamRecords(&mockErrKinesisClient{}, "AABBCC", func(data *[]byte) {
					Fail("Should not go here")
				})
			})
			Expect(exitCode).To(Equal(1))
			Expect(stderr).To(Equal("get records error: Code: Throttled"))
		})
	})
})
