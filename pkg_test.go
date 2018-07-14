package main

import (
	"bytes"
	"io"
	"os"
	"testing"

	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/golang/protobuf/proto"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/zendesk/kinesiscat/kpl"
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

var shards []*kinesis.Shard
var records []*kinesis.Record

type mockKinesisClient struct {
	kinesisiface.KinesisAPI
}

func (m *mockKinesisClient) GetRecords(*kinesis.GetRecordsInput) (output *kinesis.GetRecordsOutput, err error) {
	output = &kinesis.GetRecordsOutput{
		Records: records,
	}
	return
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

var _ = Describe("kinesiscat", func() {
	Describe("main", func() {
		It("shows help", func() {
			var actual string
			var exitCode int

			withOsArgs([]string{"kinesiscat", "--help"}, func() {
				actual = captureStdout(func() {
					exitCode = captureExit(func() {
						main()
					})
				})
			})

			Expect(actual).To(ContainSubstring("Usage:"))
			Expect(exitCode).To(Equal(2))
		})

		It("does things", func() {

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
			var exitCode int
			stderr := captureStderr(func() {
				exitCode = captureExit(func() {
					fatalfIfErr("foo: %v", err)
				})
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
			shards = []*kinesis.Shard{}

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
			// TODO
		})
	})

	Describe(".streamRecords", func() {
		It("does the thing", func() {
			mockSvc := &mockKinesisClient{}

			// TODO: actually test this

			streamRecords(mockSvc, "AABBCC", func(_ *[]byte) {

			})
		})
	})
})
