test:
	(which go-testcov >/dev/null 2>&1 || go get github.com/grosser/go-testcov) && \
	go-testcov
.PHONY: test
