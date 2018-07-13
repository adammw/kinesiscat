package main

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
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

func captureFail(fn func()) (e error) {
	e = nil
	origFailIfErr := failIfErr
	failIfErr = func(err error) {
		e = err
		if err != nil {
			panic(err)
		}
	}
	defer func() {
		failIfErr = origFailIfErr
		recover()
	}()
	fn()
	return
}

type mockKinesisClient struct {
	kinesisiface.KinesisAPI
}

func (m *mockKinesisClient) DescribeStreamPages(input *kinesis.DescribeStreamInput, fn func(*kinesis.DescribeStreamOutput, bool) bool) error {
	output := kinesis.DescribeStreamOutput{
		StreamDescription: &kinesis.StreamDescription{
			Shards: []*kinesis.Shard{},
		},
	}

	fn(&output, true)
	return nil
}

var _ = Describe("kinesiscat", func() {
	Describe("main", func() {
		It("shows help", func() {
			var actual string
			var err error

			withOsArgs([]string{"kinesiscat", "--help"}, func() {
				actual = captureStdout(func() {
					err = captureFail(func() {
						main()
					})
				})
			})

			Expect(actual).To(ContainSubstring("Usage:"))
			Expect(err).To(Not(BeNil()))
		})
	})

	Describe(".getShardIds", func() {
		It("returns empty array when no shards found", func() {
			mockSvc := &mockKinesisClient{}
			actual := getShardIds(mockSvc, "fake-stream")
			Expect(actual).To(Equal([]string{}))
		})
	})
})
