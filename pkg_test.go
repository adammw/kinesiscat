package main

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/bouk/monkey"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestSetup(t *testing.T) {
	dir, _ := os.Getwd()
	os.Setenv("FOLDER", dir)

	go main()

	RegisterFailHandler(Fail)
	RunSpecs(t, "Example")
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

func captureExit(fn func()) (exitCode int) {
	exitCode = -1
	fakeExit := func(code int) { exitCode = code }
	patch := monkey.Patch(os.Exit, fakeExit)
	defer patch.Unpatch()
	fn()
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

var _ = Describe("kinesiscat", func() {
	Describe("main", func() {
		It("shows help", func() {

			withOsArgs([]string{"kinesiscat", "-help"}, func() {
				exitCode := captureExit(func() {
					actual := captureStderr(func() {
						main()
					})
					Expect(actual).To(ContainSubstring("Usage of kinesiscat:"))
				})
				Expect(exitCode).To(Equal(2))
			})

		})
	})
})
